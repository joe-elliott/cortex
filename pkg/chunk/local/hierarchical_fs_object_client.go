package local

import (
	"context"
	"time"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
	"hash/fnv"
)

const (
	folderCount = uint64(10000)
)

// FSObjectClient holds config for filesystem as object store
type HierarchicalFSObjectClient struct {
	cfg FSConfig,
	hash hash.Hash64,
}

// NewFSObjectClient makes a chunk.ObjectClient which stores chunks as files in the local filesystem.
func NewHierarchicalFSObjectClient(cfg FSConfig) (*HierarchicalFSObjectClient, error) {
	if err := ensureDirectory(cfg.Directory); err != nil {
		return nil, err
	}

	return &HierarchicalFSObjectClient{
		cfg: cfg,
		hash: fnv.New64a(),
	}, nil
}

// Stop implements ObjectClient
func (HierarchicalFSObjectClient) Stop() {}

// PutChunks implements ObjectClient
func (f *HierarchicalFSObjectClient) PutChunks(_ context.Context, chunks []chunk.Chunk) error {	for i := range chunks {
		buf, err := chunks[i].Encoded()
		if err != nil {
			return err
		}

		filename := f.filenameFromChunk(chunks[i])
		if err := ioutil.WriteFile(filename, buf, 0644); err != nil {
			return err
		}
	}
	return nil
}

// GetChunks implements ObjectClient
func (f *HierarchicalFSObjectClient) GetChunks(ctx context.Context, chunks []chunk.Chunk) ([]chunk.Chunk, error) {
	return util.GetParallelChunks(ctx, chunks, f.getChunk)
}

func (f *HierarchicalFSObjectClient) getChunk(_ context.Context, decodeContext *chunk.DecodeContext, c chunk.Chunk) (chunk.Chunk, error) {
	filename := f.filenameFromChunk(c)
	buf, err := ioutil.ReadFile(filename)
	if err != nil {
		return c, err
	}

	if err := c.Decode(decodeContext, buf); err != nil {
		return c, err
	}

	return c, nil
}

// DeleteChunksBefore implements BucketClient
func (f *HierarchicalFSObjectClient) DeleteChunksBefore(ctx context.Context, ts time.Time) error {
	return filepath.Walk(f.cfg.Directory, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && info.ModTime().Before(ts) {
			level.Info(pkgUtil.Logger).Log("msg", "file has exceeded the retention period, removing it", "filepath", info.Name())
			if err := os.Remove(path); err != nil {
				return err
			}
		}
		return nil
	})

}

func (f *HierarchicalFSObjectClient) filenameFromChunk(c chunk.Chunk) string {
	folderName := fmt.Sprintf("%x", f.hash(c.ExternalKey) % folderCount)

	return path.Join(f.cfg.Directory, folderName, base64.StdEncoding.EncodeToString([]byte(chunks[i].ExternalKey())) 
}