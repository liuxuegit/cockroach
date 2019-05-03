package proposalquotatest

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/proposalquota"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func RunTests(t *testing.T) {
	t.Helper()
	suite{}.runTests(t)
}

func RunBenchmarks(b *testing.B) {
	b.Helper()
	suite{}.runBenchmarks(b)
}

var testingTType = reflect.TypeOf((*testing.T)(nil))
var testingBType = reflect.TypeOf((*testing.B)(nil))

func (s suite) runTests(t *testing.T) {
	sv := reflect.ValueOf(s)
	st := sv.Type()
	for i := 0; i < st.NumMethod(); i++ {
		m := st.Method(i)
		if m.Type.NumIn() == 2 && m.Type.In(1) == testingTType {
			t.Run(m.Name, sv.Method(i).Interface().(func(t *testing.T)))
		}
	}
}

func (s suite) runBenchmarks(b *testing.B) {
	sv := reflect.ValueOf(s)
	st := sv.Type()
	for i := 0; i < st.NumMethod(); i++ {
		m := st.Method(i)
		if m.Type.NumIn() == 2 && m.Type.In(1) == testingBType {
			b.Run(m.Name, sv.Method(i).Interface().(func(b *testing.B)))
		}
	}
}

type suite struct{}

// TestQuotaPoolBasic tests the minimal expected behavior of the quota pool
// with different sized quota pool and a varying number of goroutines, each
// acquiring a unit quota and releasing it immediately after.
func (s *suite) TestQuotaPoolBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	quotas := []int64{1, 10, 100, 1000}
	goroutineCounts := []int{1, 10, 100}

	for _, quota := range quotas {
		for _, numGoroutines := range goroutineCounts {
			qp := proposalquota.NewPool(quota)
			ctx := context.Background()
			resCh := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					if err := qp.Acquire(ctx, 1); err != nil {
						resCh <- err
						return
					}
					qp.Add(1)
					resCh <- nil
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				select {
				case <-time.After(5 * time.Second):
					t.Fatal("did not complete acquisitions within 5s")
				case err := <-resCh:
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			if q := qp.ApproximateQuota(); q != quota {
				t.Fatalf("expected quota: %d, got: %d", quota, q)
			}
		}
	}
}

// testQuotaPoolContextCancellation tests the behavior that for an ongoing
// blocked acquisition, if the context passed in gets canceled the acquisition
// gets canceled too with an error indicating so. This should not affect the
// available quota in the pool.
func (s suite) TestQuotaPoolContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := proposalquota.NewPool(1)
	if err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- qp.Acquire(ctx, 1)
	}()

	cancel()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellation did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
	}

	qp.Add(1)

	if q := qp.ApproximateQuota(); q != 1 {
		t.Fatalf("expected quota: 1, got: %d", q)
	}
}

// TestQuotaPoolClose tests the behavior that for an ongoing blocked
// acquisition if the quota pool gets closed, all ongoing and subsequent
// acquisitions go through.
func (suite) TestQuotaPoolClose(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	qp := proposalquota.NewPool(1)
	if err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	const numGoroutines = 5
	resCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			resCh <- qp.Acquire(ctx, 1)
		}()
	}

	qp.Close()

	// Second call should be a no-op.
	qp.Close()

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("quota pool closing did not unblock acquisitions within 5s")
		case err := <-resCh:
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	go func() {
		resCh <- qp.Acquire(ctx, 1)
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("quota pool closing did not unblock acquisitions within 5s")
	case err := <-resCh:
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestQuotaPoolCanceledAcquisitions tests the behavior where we enqueue
// multiple acquisitions with canceled contexts and expect any subsequent
// acquisition with a valid context to proceed without error.
func (suite) TestQuotaPoolCanceledAcquisitions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	qp := proposalquota.NewPool(1)
	if err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	cancel()
	const numGoroutines = 5

	errCh := make(chan error)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			errCh <- qp.Acquire(ctx, 1)
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("context cancellations did not unblock acquisitions within 5s")
		case err := <-errCh:
			if err != context.Canceled {
				t.Fatalf("expected context cancellation error, got %v", err)
			}
		}
	}

	qp.Add(1)
	go func() {
		errCh <- qp.Acquire(context.Background(), 1)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("acquisition didn't go through within 5s")
	}
}

// TestQuotaPoolNoops tests that quota pool operations that should be noops are
// so, e.g. quotaPool.acquire(0) and quotaPool.release(0).
func (suite) TestQuotaPoolNoops(t *testing.T) {
	defer leaktest.AfterTest(t)()

	qp := proposalquota.NewPool(1)
	ctx := context.Background()
	if err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error)
	go func() {
		errCh <- qp.Acquire(ctx, 1)
	}()

	qp.Add(0)
	qp.Add(1)

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("context cancellations did not unblock acquisitions within 5s")
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	}

	qp.Add(0)
	qp.Add(1)

	if err := qp.Acquire(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err := qp.Acquire(ctx, 0); err != nil {
		t.Fatal(err)
	}
}

// TestQuotaPoolMaxQuota tests that at no point does the total quota available
// in the pool exceed the maximum amount the pool was initialized with.
func (suite) TestQuotaPoolMaxQuota(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 100
	const numGoroutines = 200
	qp := proposalquota.NewPool(quota)
	ctx := context.Background()
	resCh := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			if err := qp.Acquire(ctx, 1); err != nil {
				resCh <- err
				return
			}
			qp.Add(2)
			resCh <- nil
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("did not complete acquisitions within 5s")
		case err := <-resCh:
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// TestQuotaPoolCappedAcquisition verifies that when an acquisition request
// greater than the maximum quota is placed, we still allow the acquisition to
// proceed but after having acquired the maximum quota amount.
func (suite) TestQuotaPoolCappedAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const quota = 1
	qp := proposalquota.NewPool(quota)
	if err := qp.Acquire(context.Background(), quota*100); err != nil {
		t.Fatal(err)
	}

	if q := qp.ApproximateQuota(); q != 0 {
		t.Fatalf("expected quota: %d, got: %d", 0, q)
	}

	qp.Add(quota)

	if q := qp.ApproximateQuota(); q != quota {
		t.Fatalf("expected quota: %d, got: %d", quota, q)
	}
}

// BenchmarkQuotaPool benchmarks the common case where we have sufficient
// quota available in the pool and we repeatedly acquire and release quota.
func (suite) BenchmarkQuotaPool(b *testing.B) {
	qp := proposalquota.NewPool(1)
	ctx := context.Background()
	for n := 0; n < b.N; n++ {
		if err := qp.Acquire(ctx, 1); err != nil {
			b.Fatal(err)
		}
		qp.Add(1)
	}
	qp.Close()
}
