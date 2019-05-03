package quotapool_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/proposalquota/testutils/proposalquotatest"
)

func Test(t *testing.T) { proposalquotatest.RunTests(t) }

func Benchmark(b *testing.B) { proposalquotatest.RunBenchmarks(b) }
