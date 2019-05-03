package proposalquota

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
)

// New creates a new ProposalQuota with a maximum number of bytes.
func NewPool(max int64) *Pool {
	p := Pool{
		max: quota(max),
		quotaSyncPool: sync.Pool{
			New: func() interface{} { q := quota(0); return &q },
		},
		requestSyncPool: sync.Pool{
			New: func() interface{} { return &request{} },
		},
	}
	p.qp = quotapool.New("proposal", (*pool)(&p))
	return &p
}

type Pool struct {
	qp              *quotapool.QuotaPool
	max             quota
	quotaSyncPool   sync.Pool
	requestSyncPool sync.Pool
}

func (qp *Pool) Acquire(ctx context.Context, v int64) error {
	r := qp.newRequest(v)
	defer qp.putRequest(r)
	return qp.qp.Acquire(ctx, r)
}

func (qp *Pool) Add(v int64) {
	vv := qp.quotaSyncPool.Get().(*quota)
	*vv = quota(v)
	qp.qp.Add(vv)
}

func (qp *Pool) ApproximateQuota() int64 {
	q := qp.qp.ApproximateQuota()
	if q == nil {
		return 0
	}
	return int64(*q.(*quota))
}

func (qp *Pool) Close() {
	qp.qp.Close()
}

type pool Pool

func (p *Pool) newRequest(v int64) *request {
	r := p.requestSyncPool.Get().(*request)
	r.want = quota(v)
	if r.want > p.max {
		r.want = p.max
	}
	r.got = p.quotaSyncPool.Get().(*quota)
	return r
}

func (qp *Pool) putRequest(r *request) {
	*r.got = 0
	qp.quotaSyncPool.Put(r.got)
	qp.requestSyncPool.Put(r)
}

func (p *pool) InitialQuota() quotapool.Quota {
	q := p.quotaSyncPool.Get().(*quota)
	*q = p.max
	return q
}

func (p *pool) Merge(a, b quotapool.Quota) quotapool.Quota {
	//	fmt.Printf("%v %p %v %p\n", a, a, b, b)
	aa, bb := a.(*quota), b.(*quota)
	*aa += *bb
	*bb = 0
	if *aa > p.max {
		*aa = p.max
	}
	p.quotaSyncPool.Put(b)
	return a
}

type quota int64

type request struct {
	want quota
	got  *quota
}

func (q *quota) String() string {
	if q == nil {
		return humanizeutil.IBytes(0)
	}
	return humanizeutil.IBytes(int64(*q))
}

func (r *request) Acquire(p quotapool.Pool, v quotapool.Quota) (extra quotapool.Quota) {
	vq := v.(*quota)
	*r.got += *vq
	//fmt.Printf("%p %v %v %v\n", v, v, &r.want, r.got)
	if *r.got > r.want {
		*vq = *r.got - r.want
		*r.got = r.want
		return vq
	}
	*vq = 0
	p.(*pool).quotaSyncPool.Put(vq)
	return nil
}

func (r *request) Acquired() quotapool.Quota { return r.got }

func (r *request) Fulfilled() bool { return *r.got == r.want }
