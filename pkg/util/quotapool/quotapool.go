package quotapool

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// QuotaPool is an abstract implementation of a pool that stores some unit of
// quota. The basic idea is that it allows requests to acquire a quantity of
// abstract Quota from the pool while playing nice with concepts with context
// cancelation. Quota is some unit which can be merged together via a Pool and
// can be acquired by a request.
type QuotaPool struct {
	// name is used for logging and is not modified.
	name string

	// We use a channel to 'park' our quota value for easier composition with
	// context cancellation and quotaPool closing (see quotaPool.acquire).
	//
	// Quota additions push a value into the channel whereas the acquisition
	// first in line waits on the channel itself.
	quota chan Quota
	pool  Pool

	// Ongoing acquisitions listen on quotaPool.done which is closed when the quota
	// pool is closed (see QuotaPool.Close).
	done chan struct{}

	mu struct {
		syncutil.Mutex

		// We service quota acquisitions in a first come, first serve basis. This
		// is done in order to prevent starvations of large acquisitions by a
		// continuous stream of smaller ones. Acquisitions 'register' themselves
		// for a notification that indicates they're now first in line. This is
		// done by appending to the queue the channel they will then wait
		// on. If a goroutine no longer needs to be notified, i.e. their
		// acquisition context has been canceled, the goroutine is responsible for
		// blocking subsequent notifications to the channel by filling up the
		// channel buffer.
		queue []chan struct{}

		// closed is set to true when the quota pool is closed (see
		// QuotaPool.Close).
		closed bool
	}
}

// Pool deal with providing the creation and lifecycle of Quota objects.
// It provides the initial Quota and then deals with merging Quota when it
// it returned.
//
// Generally implementes will use implement Request and Quota in a way such
// that when request acquisitions occur they can properly manage the allocation
// lifecycle for proposal objects.
type Pool interface {
	InitialQuota() Quota
	Merge(a, b Quota) Quota
}

// Quota is an abstract interface that represents a quantity of Quota.
type Quota interface{}

// Request is an interface used to acquire quota from the pool.
type Request interface {
	Acquire(Pool, Quota) (extra Quota)
	Acquired() Quota
	Fulfilled() bool
}

// New returns a new quota pool initialized with a given quota. The quota
// is capped at this amount, meaning that callers may return more quota than they
// acquired without ever making more than the quota capacity available.
func New(name string, pool Pool) *QuotaPool {
	qp := &QuotaPool{
		name:  name,
		quota: make(chan Quota, 1),
		done:  make(chan struct{}),
		pool:  pool,
	}
	qp.quota <- pool.InitialQuota()
	return qp
}

// add adds the specified quota back to the pool. At no point does the total
// quota in the pool exceed the maximum capacity determined during
// initialization.
//
// Safe for concurrent use.
func (qp *QuotaPool) Add(v Quota) {
	qp.mu.Lock()
	defer qp.mu.Unlock()
	qp.addLocked(v)
}

// addLocked is like add, but it requires that qp.Lock is held.
func (qp *QuotaPool) addLocked(v Quota) {
	select {
	case q := <-qp.quota:
		v = qp.pool.Merge(v, q)
	default:
	}
	qp.quota <- v
}

func logSlowQuota(ctx context.Context, name string, r Request, start time.Time) func() {
	log.Warningf(ctx, "have been waiting %s attempting to acquire %s of %s quota",
		timeutil.Since(start), name, r)
	return func() {
		log.Infof(ctx, "acquired %s of %v quota after %s",
			r, name, timeutil.Since(start))
	}
}

var chanPool = sync.Pool{
	New: func() interface{} { return make(chan struct{}, 1) },
}

type queueElem struct {
	ch   chan struct{}
	next *queueElem
}

// acquire acquires the specified amount of quota from the pool. On success,
// nil is returned and the caller must call add(v) or otherwise arrange for the
// quota to be returned to the pool. If 'v' is greater than the total capacity
// of the pool, we instead try to acquire quota equal to the maximum capacity.
//
// Safe for concurrent use.
func (qp *QuotaPool) Acquire(ctx context.Context, r Request) error {
	notifyCh := chanPool.Get().(chan struct{})
	qp.mu.Lock()
	qp.mu.queue = append(qp.mu.queue, notifyCh)
	// If we're first in line, we notify ourself immediately.
	if len(qp.mu.queue) == 1 {
		notifyCh <- struct{}{}
	}
	qp.mu.Unlock()
	slowTimer := timeutil.NewTimer()
	defer slowTimer.Stop()
	start := timeutil.Now()

	// Intentionally reset only once, for we care more about the select duration in
	// goroutine profiles than periodic logging.
	slowTimer.Reset(base.SlowRequestThreshold)
	for {
		select {
		case <-slowTimer.C:
			slowTimer.Read = true
			defer logSlowQuota(ctx, qp.name, r, start)()
			continue
		case <-ctx.Done():
			qp.mu.Lock()
			// We no longer need to be notified but we need to be careful and check
			// whether or not we're first in queue. If so, we need to notify the
			// next acquisition goroutine and clean up the waiting queue while
			// doing so.
			//
			// Else we simply 'unregister' ourselves from the queue by filling
			// up the channel buffer. This is what is checked when a goroutine
			// wishes to notify the next in line.
			if qp.mu.queue[0] == notifyCh {
				// NB: Notifying the channel before moving it to the head of the
				// queue is safe because the queue itself is guarded by a lock.
				// Goroutines are not a risk of getting notified and finding
				// out they're not first in line.
				qp.notifyNextLocked()
			} else {
				notifyCh <- struct{}{}
			}

			qp.mu.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to 'unregister' ourselves as in the case when the
			// context is canceled. In fact, we want others waiters to only
			// receive on qp.done and signaling them would work against that.
			return nil
		case <-notifyCh:
			chanPool.Put(notifyCh)
		}
		break
	}

	// We're first in line to receive quota, we keep accumulating quota until
	// we've acquired enough or determine we no longer need the acquisition.
	// If we have acquired the quota needed or our context gets canceled,
	// we're sure to remove ourselves from the queue and notify the goroutine
	// next in line (if any).
	var extra Quota
	var acquiredAny bool
	for !r.Fulfilled() {
		select {
		case <-slowTimer.C:
			slowTimer.Read = true
			defer logSlowQuota(ctx, qp.name, r, start)()
		case <-ctx.Done():
			qp.mu.Lock()
			if acquiredAny {
				qp.addLocked(r.Acquired())
			}
			qp.notifyNextLocked()
			qp.mu.Unlock()
			return ctx.Err()
		case <-qp.done:
			// We don't need to release quota back as all ongoing and
			// subsequent acquisitions will succeed immediately.
			return nil
		case q := <-qp.quota:
			acquiredAny = true
			extra = r.Acquire(qp.pool, q)
		}
	}
	qp.mu.Lock()
	if extra != nil {
		qp.addLocked(extra)
	}
	qp.notifyNextLocked()
	qp.mu.Unlock()
	return nil
}

// notifyNextLocked notifies the waiting acquisition goroutine next in line (if
// any). It requires that qp.Lock is held.
func (qp *QuotaPool) notifyNextLocked() {
	// We're at the head of the queue. We traverse until we find a goroutine
	// waiting to be notified, notify the goroutine and truncate our queue so
	// to ensure the said goroutine is at the head of the queue. Normally the
	// next lined up waiter is the one waiting for notification, but if others
	// behind us have also gotten their context canceled, they
	// will leave behind waiters that we would skip below.
	//
	// If we determine there are no goroutines waiting, we simply truncate the
	// queue to reflect this.
	qp.mu.queue = qp.mu.queue[1:]
	for _, ch := range qp.mu.queue {
		select {
		case ch <- struct{}{}:
		default:
			chanPool.Put(ch)
			qp.mu.queue = qp.mu.queue[1:]
			continue
		}
		break
	}
}

// ApproximateQuota will correctly report approximately the amount of quota
// available in the pool. It is accurate only if there are no ongoing
// acquisition goroutines. If there are, the return value can be up to 'v' less
// than actual available quota where 'v' is the value the acquisition goroutine
// first in line is attempting to acquire.
func (qp *QuotaPool) ApproximateQuota() Quota {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	select {
	case q := <-qp.quota:
		qp.quota <- q
		return q
	default:
		return nil
	}
}

// Close signals to all ongoing and subsequent acquisitions that they are
// free to return to their callers without error.
//
// Safe for concurrent use.
func (qp *QuotaPool) Close() {
	qp.mu.Lock()
	if !qp.mu.closed {
		qp.mu.closed = true
		close(qp.done)
	}
	qp.mu.Unlock()
}
