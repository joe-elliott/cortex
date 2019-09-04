package local

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
)

// FSObjectClient holds config for filesystem as object store
type HierarchicalFSObjectClient struct {
	cfg FSConfig
}

// NewFSObjectClient makes a chunk.ObjectClient which stores chunks as files in the local filesystem.
func NewHierarchicalFSObjectClient(cfg FSConfig) (*HierarchicalFSObjectClient, error) {
	if err := ensureDirectory(cfg.Directory); err != nil {
		return nil, err
	}

	return &HierarchicalFSObjectClient{
		cfg: cfg,
	}, nil
}

// Stop implements ObjectClient
func (HierarchicalFSObjectClient) Stop() {}

// PutChunks implements ObjectClient
func (f *HierarchicalFSObjectClient) PutChunks(_ context.Context, chunks []chunk.Chunk) error {
	return nil
}

// GetChunks implements ObjectClient
func (f *HierarchicalFSObjectClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, f.getChunk)
}

func (f *HierarchicalFSObjectClient) getChunk(_ context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	return chunk.Chunk{}, nil
}

// DeleteChunksBefore implements BucketClient
func (f *HierarchicalFSObjectClient) DeleteChunksBefore(ctx context.Context, ts time.Time) error {
	return nil
}
