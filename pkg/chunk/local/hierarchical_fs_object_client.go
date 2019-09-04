package local

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"hash"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"time"

	"hash/fnv"

	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/cortexproject/cortex/pkg/chunk/util"
	pkgUtil "github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
)

const (
	folderCount = uint64(10000)
)

// HierarchicalFSConfig is the config for a FSObjectClient.
type HierarchicalFSConfig struct {
	Directory string `yaml:"directory"`
}

// RegisterFlags registers flags.
func (cfg *HierarchicalFSConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Directory, "local.hierarchical-chunk-directory", "", "Directory to store chunks in.")
}

// FSObjectClient holds config for filesystem as object store
type HierarchicalFSObjectClient struct {
	cfg  FSConfig
	hash hash.Hash64
}

// NewFSObjectClient makes a chunk.ObjectClient which stores chunks as files in the local filesystem.
func NewHierarchicalFSObjectClient(cfg FSConfig) (*HierarchicalFSObjectClient, error) {
	if err := ensureDirectory(cfg.Directory); err != nil {
		return nil, err
	}

	return &HierarchicalFSObjectClient{
		cfg:  cfg,
		hash: fnv.New64a(),
	}, nil
}

// Stop implements ObjectClient
func (HierarchicalFSObjectClient) Stop() {}

// PutChunks implements ObjectClient
func (f *HierarchicalFSObjectClient) PutChunks(_ context.Context, chunks []chunk.Chunk) error {
	for i := range chunks {
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
	f.hash.Write([]byte(c.ExternalKey()))

	folderName := fmt.Sprintf("%x", f.hash.Sum64()%folderCount)

	return path.Join(f.cfg.Directory, folderName, base64.StdEncoding.EncodeToString([]byte(c.ExternalKey())))
}
