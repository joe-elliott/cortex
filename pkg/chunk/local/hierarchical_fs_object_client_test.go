package local

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cortexproject/cortex/pkg/chunk/testutils"

	"github.com/prometheus/common/model"
)

func TestPutChunks(t *testing.T) {

	fsChunksDir, err := ioutil.TempDir(os.TempDir(), "fs-chunks")
	require.NoError(t, err)

	client, err := NewHierarchicalFSObjectClient(HierarchicalFSConfig{
		Directory: fsChunksDir,
	})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.RemoveAll(fsChunksDir))
	}()

	_, chunks, err := testutils.CreateChunks(0, 10, model.Now())
	require.NoError(t, err)

	err = client.PutChunks(context.Context(nil), chunks)
	require.NoError(t, err)
}

func BenchmarkHierarchicalFSObjectClient_PutChunks(b *testing.B) {

	fsChunksDir, err := ioutil.TempDir(os.TempDir(), "fs-chunks")
	require.NoError(b, err)

	client, err := NewHierarchicalFSObjectClient(HierarchicalFSConfig{
		Directory: fsChunksDir,
	})
	require.NoError(b, err)

	defer func() {
		require.NoError(b, os.RemoveAll(fsChunksDir))
	}()

	for i := 0; i < b.N; i++ {
		_, chunks, err := testutils.CreateChunks(0, 10, model.Now())
		require.NoError(b, err)

		err = client.PutChunks(context.Context(nil), chunks)
		require.NoError(b, err)
	}
}

/*func TestHierarchicalFsObjectClient_DeleteChunksBefore(t *testing.T) {
	deleteFilesOlderThan := 10 * time.Minute

	fsChunksDir, err := ioutil.TempDir(os.TempDir(), "fs-chunks")
	require.NoError(t, err)

	bucketClient, err := NewFSObjectClient(FSConfig{
		Directory: fsChunksDir,
	})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, os.RemoveAll(fsChunksDir))
	}()

	file1 := "file1"
	file2 := "file2"

	// Creating dummy files
	require.NoError(t, os.Chdir(fsChunksDir))

	f, err := os.Create(file1)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	f, err = os.Create(file2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Verify whether all files are created
	files, _ := ioutil.ReadDir(".")
	require.Equal(t, 2, len(files), "Number of files should be 2")

	// No files should be deleted, since all of them are not much older
	require.NoError(t, bucketClient.DeleteChunksBefore(context.Background(), time.Now().Add(-deleteFilesOlderThan)))
	files, _ = ioutil.ReadDir(".")
	require.Equal(t, 2, len(files), "Number of files should be 2")

	// Changing mtime of file1 to make it look older
	require.NoError(t, os.Chtimes(file1, time.Now().Add(-deleteFilesOlderThan), time.Now().Add(-deleteFilesOlderThan)))
	require.NoError(t, bucketClient.DeleteChunksBefore(context.Background(), time.Now().Add(-deleteFilesOlderThan)))

	// Verifying whether older file got deleted
	files, _ = ioutil.ReadDir(".")
	require.Equal(t, 1, len(files), "Number of files should be 1 after enforcing retention")
}*/
