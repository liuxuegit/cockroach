// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ledger

import (
	gosql "database/sql"
	"hash/fnv"
	"math/rand"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload"
)

type ledger struct {
	flags     workload.Flags
	connFlags *workload.ConnFlags

	seed          int64
	customers     int
	parallelStmts bool
	interleaved   bool
	splits        int
	mix           string

	txs  []tx
	deck []int // contains indexes into the txs slice

	reg      *workload.HistogramRegistry
	rngPool  *sync.Pool
	hashPool *sync.Pool
}

func init() {
	workload.Register(ledgerMeta)
}

var ledgerMeta = workload.Meta{
	Name:        `ledger`,
	Description: `Ledger simulates an accounting system using double-entry bookkeeping`,
	Version:     `1.0.0`,
	New: func() workload.Generator {
		g := &ledger{}
		g.flags.FlagSet = pflag.NewFlagSet(`ledger`, pflag.ContinueOnError)
		g.connFlags = workload.NewConnFlags(&g.flags)
		g.flags.Int64Var(&g.seed, `seed`, 1, `Random number generator seed`)
		g.flags.IntVar(&g.customers, `customers`, 1000, `Number of customers`)
		g.flags.BoolVar(&g.parallelStmts, `parallel-stmts`, false, `Use parallel statement execution`)
		g.flags.BoolVar(&g.interleaved, `interleaved`, false, `Use interleaved tables`)
		g.flags.IntVar(&g.splits, `splits`, 0, `Number of splits to perform before starting normal operations`)
		g.flags.StringVar(&g.mix, `mix`,
			`balance=50,withdrawal=37,deposit=12,reversal=0`,
			`Weights for the transaction mix.`)
		return g
	},
}

// Meta implements the Generator interface.
func (*ledger) Meta() workload.Meta { return ledgerMeta }

// Flags implements the Flagser interface.
func (w *ledger) Flags() workload.Flags { return w.flags }

// Hooks implements the Hookser interface.
func (w *ledger) Hooks() workload.Hooks {
	return workload.Hooks{
		Validate: func() error {
			if w.interleaved {
				return errors.Errorf("interleaved tables are not yet supported")
			}
			return initializeMix(w)
		},
	}
}

// Tables implements the Generator interface.
func (w *ledger) Tables() []workload.Table {
	if w.rngPool == nil {
		w.rngPool = &sync.Pool{
			New: func() interface{} { return rand.New(rand.NewSource(timeutil.Now().UnixNano())) },
		}
	}
	if w.hashPool == nil {
		w.hashPool = &sync.Pool{
			New: func() interface{} { return fnv.New64() },
		}
	}

	customer := workload.Table{
		Name:   `customer`,
		Schema: ledgerCustomerSchema,
		InitialRows: workload.Tuples(
			w.customers,
			w.ledgerCustomerInitialRow,
		),
		Splits: workload.Tuples(
			numTxnsPerCustomer*w.splits,
			w.ledgerCustomerSplitRow,
		),
	}
	transaction := workload.Table{
		Name:   `transaction`,
		Schema: ledgerTransactionSchema,
		InitialRows: workload.Tuples(
			numTxnsPerCustomer*w.customers,
			w.ledgerTransactionInitialRow,
		),
		Splits: workload.Tuples(
			w.splits,
			w.ledgerTransactionSplitRow,
		),
	}
	entry := workload.Table{
		Name:   `entry`,
		Schema: ledgerEntrySchema,
		InitialRows: workload.Tuples(
			numEntriesPerCustomer*w.customers,
			w.ledgerEntryInitialRow,
		),
		Splits: workload.Tuples(
			numEntriesPerCustomer*w.splits,
			w.ledgerEntrySplitRow,
		),
	}
	session := workload.Table{
		Name:   `session`,
		Schema: ledgerSessionSchema,
		InitialRows: workload.Tuples(
			w.customers,
			w.ledgerSessionInitialRow,
		),
		Splits: workload.Tuples(
			w.splits,
			w.ledgerSessionSplitRow,
		),
	}
	return []workload.Table{
		customer, transaction, entry, session,
	}
}

// Ops implements the Opser interface.
func (w *ledger) Ops(urls []string, reg *workload.HistogramRegistry) (workload.QueryLoad, error) {
	sqlDatabase, err := workload.SanitizeUrls(w, w.connFlags.DBOverride, urls)
	if err != nil {
		return workload.QueryLoad{}, err
	}
	db, err := gosql.Open(`cockroach`, strings.Join(urls, ` `))
	if err != nil {
		return workload.QueryLoad{}, err
	}
	// Allow a maximum of concurrency+1 connections to the database.
	db.SetMaxOpenConns(w.connFlags.Concurrency + 1)
	db.SetMaxIdleConns(w.connFlags.Concurrency + 1)

	w.reg = reg
	ql := workload.QueryLoad{SQLDatabase: sqlDatabase}
	for i := 0; i < w.connFlags.Concurrency; i++ {
		worker := &worker{
			config:   w,
			hists:    reg.GetHandle(),
			db:       db,
			deckPerm: append([]int(nil), w.deck...),
			permIdx:  len(w.deck),
		}
		ql.WorkerFns = append(ql.WorkerFns, worker.run)
	}
	return ql, nil
}
