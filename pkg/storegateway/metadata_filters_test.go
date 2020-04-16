package storegateway

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/extprom"

	"github.com/cortexproject/cortex/pkg/ring"
	"github.com/cortexproject/cortex/pkg/ring/kv/consul"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util/services"
)

func TestShardingMetadataFilter(t *testing.T) {
	// The following block IDs have been picked to have increasing hash values
	// in order to simplify the tests.
	block1 := ulid.MustNew(1, nil) // hash: 283204220
	block2 := ulid.MustNew(2, nil) // hash: 444110359
	block3 := ulid.MustNew(5, nil) // hash: 2931974232
	block4 := ulid.MustNew(6, nil) // hash: 3092880371
	numAllBlocks := 4

	block1Hash := cortex_tsdb.HashBlockID(block1)
	block2Hash := cortex_tsdb.HashBlockID(block2)
	block3Hash := cortex_tsdb.HashBlockID(block3)
	block4Hash := cortex_tsdb.HashBlockID(block4)

	tests := map[string]struct {
		replicationFactor int
		setupRing         func(*ring.Desc)
		expectedBlocks    map[string][]ulid.ULID
	}{
		"one ACTIVE instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {},
			},
		},
		"two ACTIVE instances in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3},
				"127.0.0.2": {block2, block4},
			},
		},
		"one ACTIVE instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{0}, ring.ACTIVE)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {},
			},
		},
		"two ACTIVE instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1, block4Hash + 1}, ring.ACTIVE)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block2, block3, block4},
				"127.0.0.2": {block1, block2, block3, block4},
			},
		},
		"multiple ACTIVE instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.ACTIVE)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block2, block4},
				"127.0.0.2": {block2 /* replicated: */, block1},
				"127.0.0.3": {block4 /* replicated: */, block3},
			},
		},
		"one unhealthy instance in the ring with replication factor = 1": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)

				r.Ingesters["instance-3"] = ring.IngesterDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// No shard has the blocks of the unhealthy instance.
				"127.0.0.1": {block1, block3},
				"127.0.0.2": {block2},
				"127.0.0.3": {},
			},
		},
		"one unhealthy instance in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)

				r.Ingesters["instance-3"] = ring.IngesterDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block2, block4},
				"127.0.0.2": {block2 /* replicated: */, block1},
				"127.0.0.3": {},
			},
		},
		"two unhealthy instances in the ring with replication factor = 2": {
			replicationFactor: 2,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)

				r.Ingesters["instance-2"] = ring.IngesterDesc{
					Addr:      "127.0.0.2",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block2Hash + 1, block3Hash + 1},
				}

				r.Ingesters["instance-3"] = ring.IngesterDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// There may be some blocks missing depending if there are shared blocks
				// between the two unhealthy nodes.
				"127.0.0.1": {block1 /* replicated: */, block4},
				"127.0.0.2": {},
				"127.0.0.3": {},
			},
		},
		"two unhealthy instances in the ring with replication factor = 3": {
			replicationFactor: 3,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)

				r.Ingesters["instance-3"] = ring.IngesterDesc{
					Addr:      "127.0.0.3",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block3Hash + 1},
				}

				r.Ingesters["instance-4"] = ring.IngesterDesc{
					Addr:      "127.0.0.4",
					Timestamp: time.Now().Add(-time.Hour).Unix(),
					State:     ring.ACTIVE,
					Tokens:    []uint32{block4Hash + 1},
				}
			},
			expectedBlocks: map[string][]ulid.ULID{
				// There may be some blocks missing depending if there are shared blocks
				// between the two unhealthy nodes.
				"127.0.0.1": {block1 /* replicated: */, block3, block4},
				"127.0.0.2": {block2 /* replicated: */, block1, block4},
				"127.0.0.3": {},
				"127.0.0.4": {},
			},
		},
		"LEAVING instance in the ring should continue to keep its shard blocks but they should also be replicated to another instance": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.LEAVING)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block4},
				"127.0.0.2": {block2},
				"127.0.0.3": {block4},
			},
		},
		"JOINING instance in the ring should get its shard blocks but they should also be replicated to another instance": {
			replicationFactor: 1,
			setupRing: func(r *ring.Desc) {
				r.AddIngester("instance-1", "127.0.0.1", "", []uint32{block1Hash + 1, block3Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-2", "127.0.0.2", "", []uint32{block2Hash + 1}, ring.ACTIVE)
				r.AddIngester("instance-3", "127.0.0.3", "", []uint32{block4Hash + 1}, ring.JOINING)
			},
			expectedBlocks: map[string][]ulid.ULID{
				"127.0.0.1": {block1, block3 /* replicated: */, block4},
				"127.0.0.2": {block2},
				"127.0.0.3": {block4},
			},
		},
	}

	for testName, testData := range tests {
		testName := testName
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			store := consul.NewInMemoryClient(ring.GetCodec())

			// Initialize the ring state.
			require.NoError(t, store.CAS(ctx, "test", func(in interface{}) (interface{}, bool, error) {
				d := ring.NewDesc()
				testData.setupRing(d)
				return d, true, nil
			}))

			cfg := ring.Config{
				ReplicationFactor: testData.replicationFactor,
				HeartbeatTimeout:  time.Minute,
			}

			r, err := ring.NewWithStoreClientAndStrategy(cfg, "test", "test", store, &BlocksReplicationStrategy{})
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, r))
			defer services.StopAndAwaitTerminated(ctx, r) //nolint:errcheck

			// Wait until the ring client has synced.
			require.NoError(t, ring.WaitInstanceState(ctx, r, "instance-1", ring.ACTIVE))

			for instanceAddr, expectedBlocks := range testData.expectedBlocks {
				filter := NewShardingMetadataFilter(r, instanceAddr, log.NewNopLogger())
				synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{}, []string{"state"})
				synced.WithLabelValues(shardExcludedMeta).Set(0)

				metas := map[ulid.ULID]*metadata.Meta{
					block1: {},
					block2: {},
					block3: {},
					block4: {},
				}

				err = filter.Filter(ctx, metas, synced, false)
				require.NoError(t, err)

				var actualBlocks []ulid.ULID
				for id := range metas {
					actualBlocks = append(actualBlocks, id)
				}

				assert.ElementsMatch(t, expectedBlocks, actualBlocks)

				// Assert on the metric used to keep track of the blocks filtered out.
				synced.Submit()
				assert.Equal(t, float64(numAllBlocks-len(testData.expectedBlocks[instanceAddr])), testutil.ToFloat64(synced))
			}
		})
	}
}
