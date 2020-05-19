package server

import (
	"archive/tar"
	"hash"
	"io"
	"log"
	"path"
	"regexp"
	"strconv"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	globlib "github.com/pachyderm/ohmyglob"
	"github.com/pachyderm/pachyderm/src/client"
	"github.com/pachyderm/pachyderm/src/client/auth"
	"github.com/pachyderm/pachyderm/src/client/pfs"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	pfsserver "github.com/pachyderm/pachyderm/src/server/pfs"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset"
	"github.com/pachyderm/pachyderm/src/server/pkg/storage/fileset/index"
	txnenv "github.com/pachyderm/pachyderm/src/server/pkg/transactionenv"
	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
	"github.com/pachyderm/pachyderm/src/server/pkg/work"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

const (
	// tmpPrefix is for temporary storage paths.
	tmpPrefix            = "tmp"
	storageTaskNamespace = "storage"
)

func (d *driver) startCommitNewStorageLayer(txnCtx *txnenv.TransactionContext, id string, parent *pfs.Commit, branch string, provenance []*pfs.CommitProvenance, description string) (*pfs.Commit, error) {
	return d.startCommit(txnCtx, id, parent, branch, provenance, description)
}

func (d *driver) finishCommitNewStorageLayer(txnCtx *txnenv.TransactionContext, commit *pfs.Commit, description string) (retErr error) {
	if err := d.checkIsAuthorizedInTransaction(txnCtx, commit.Repo, auth.Scope_WRITER); err != nil {
		return err
	}
	commitInfo, err := d.resolveCommit(txnCtx.Stm, commit)
	if err != nil {
		return err
	}
	if commitInfo.Finished != nil {
		return pfsserver.ErrCommitFinished{commit}
	}
	if description != "" {
		commitInfo.Description = description
	}
	commitPath := path.Join(commit.Repo.Name, commit.ID)
	// Clean up temporary filesets leftover from failed operations.
	if err := d.storage.Delete(txnCtx.Client.Ctx(), path.Join(tmpPrefix, commitPath)); err != nil {
		return err
	}
	// Run compaction task.
	return d.compactionQueue.RunTaskBlock(txnCtx.Client.Ctx(), func(m *work.Master) error {
		if err := backoff.Retry(func() error {
			if err := d.storage.Delete(context.Background(), path.Join(commitPath, fileset.Diff)); err != nil {
				return err
			}
			return d.storage.Delete(context.Background(), path.Join(commitPath, fileset.Compacted))
		}, backoff.NewExponentialBackOff()); err != nil {
			return err
		}
		// Compact the commit changes into a diff file set.
		if err := d.compact(context.Background(), m, path.Join(commitPath, fileset.Diff), []string{commitPath}); err != nil {
			return err
		}
		// Compact the commit changes (diff file set) into the total changes in the commit's ancestry.
		var compactSpec *fileset.CompactSpec
		if commitInfo.ParentCommit == nil {
			compactSpec, err = d.storage.CompactSpec(m.Ctx(), commitPath)
		} else {
			parentCommitPath := path.Join(commitInfo.ParentCommit.Repo.Name, commitInfo.ParentCommit.ID)
			compactSpec, err = d.storage.CompactSpec(m.Ctx(), commitPath, parentCommitPath)
		}
		if err != nil {
			return err
		}
		if err := d.compact(context.Background(), m, compactSpec.Output, compactSpec.Input); err != nil {
			return err
		}
		// (bryce) need size.
		commitInfo.SizeBytes = uint64(0)
		commitInfo.Finished = types.TimestampNow()
		return d.writeFinishedCommit(txnCtx.Stm, commit, commitInfo)
	})
}

// (bryce) holding off on integrating with downstream commit deletion logic until global ids.
//func (d *driver) deleteCommitNewStorageLayer(txnCtx *txnenv.TransactionContext, commit *pfs.Commit) error {
//	return d.storage.Delete(txnCtx.Client.Ctx(), path.Join(commit.Repo.Name, commit.ID))
//}

// (bryce) add commit validation.
func (d *driver) withFileSet(ctx context.Context, repo, commit string, f func(*fileset.FileSet) error) (retErr error) {
	// (bryce) subFileSet will need to be incremented through etcd eventually.
	d.mu.Lock()
	subFileSetStr := fileset.SubFileSetStr(d.subFileSet)
	subFileSetPath := path.Join(repo, commit, subFileSetStr)
	fs := d.storage.New(ctx, path.Join(tmpPrefix, subFileSetPath), subFileSetStr)
	d.subFileSet++
	d.mu.Unlock()
	defer func() {
		if err := d.storage.Delete(ctx, path.Join(tmpPrefix, subFileSetPath)); retErr == nil {
			retErr = err
		}
	}()
	if err := f(fs); err != nil {
		return err
	}
	if err := fs.Close(); err != nil {
		return err
	}
	return d.compactionQueue.RunTaskBlock(ctx, func(m *work.Master) error {
		return d.compact(ctx, m, subFileSetPath, []string{path.Join(tmpPrefix, subFileSetPath)})
	})
}

func (d *driver) getFilesNewStorageLayer(ctx context.Context, repo, commit, glob string, w io.Writer) error {
	if err := d.getFilesConditional(ctx, repo, commit, glob, func(fr *FileReader) error {
		return fr.Get(w, true)
	}); err != nil {
		return err
	}
	// Close a tar writer to create tar EOF padding.
	return tar.NewWriter(w).Close()
}

func (d *driver) listFileV2(pachClient *client.APIClient, file *pfs.File, full bool, history int64, f func(*pfs.FileInfoNewStorage) error) (retErr error) {
	ctx := pachClient.Ctx()
	if err := validateFile(file); err != nil {
		return err
	}
	if err := d.checkIsAuthorized(pachClient, file.Commit.Repo, auth.Scope_READER); err != nil {
		return err
	}
	return d.getFilesConditional(ctx, file.Commit.Repo.Name, file.Commit.ID, file.Path, func(fr *FileReader) error {
		return f(fr.Info())
	})
}

var globRegex = regexp.MustCompile(`[*?[\]{}!()@+^]`)

func globLiteralPrefix(glob string) string {
	idx := globRegex.FindStringIndex(glob)
	if idx == nil {
		return glob
	}
	return glob[:idx[0]]
}

// (bryce) glob should be cleaned in option function
func (d *driver) getFilesConditional(ctx context.Context, repo, commit, glob string, f func(*FileReader) error) error {
	compactedPaths := []string{path.Join(repo, commit, fileset.Compacted)}
	prefix := globLiteralPrefix(glob)
	mr, err := d.storage.NewMergeReader(ctx, compactedPaths, index.WithPrefix(prefix))
	if err != nil {
		return err
	}
	mf, err := matchFunc(glob)
	if err != nil {
		return err
	}
	var fr *FileReader
	nextFileReader := func(idx *index.Index) error {
		fmr, err := mr.Next()
		if err != nil {
			return err
		}
		if !mf(idx.Path) {
			return nil
		}
		fr = newFileReader(client.NewFile(repo, commit, idx.Path), idx, fmr, mr)
		return nil
	}
	if err := d.storage.ResolveIndexes(ctx, compactedPaths, func(idx *index.Index) error {
		if fr == nil {
			return nextFileReader(idx)
		}
		dir := path.Dir(idx.Path)
		if dir == fr.file.Path {
			fr.updateFileInfo(idx)
			return nil
		}
		if err := f(fr); err != nil {
			return err
		}
		if err := fr.drain(); err != nil {
			return err
		}
		fr = nil
		return nextFileReader(idx)

	}, index.WithPrefix(prefix)); err != nil {
		return err
	}
	if fr != nil {
		return f(fr)
	}
	return nil
}

func matchFunc(glob string) (func(string) bool, error) {
	// (bryce) this is a little weird, but it prevents the parent directory from being matched (i.e. /*).
	var parentG *globlib.Glob
	parentGlob, baseGlob := path.Split(glob)
	if len(baseGlob) > 0 {
		var err error
		parentG, err = globlib.Compile(parentGlob, '/')
		if err != nil {
			return nil, err
		}
	}
	g, err := globlib.Compile(glob, '/')
	if err != nil {
		return nil, err
	}
	return func(s string) bool {
		return g.Match(s) && (parentG == nil || !parentG.Match(s))
	}, nil
}

// FileReader is a PFS wrapper for a fileset.MergeReader.
// The primary purpose of this abstraction is to convert from index.Index to
// pfs.FileInfoNewStorage and to convert a set of index hashes to a file hash.
type FileReader struct {
	file      *pfs.File
	idx       *index.Index
	fmr       *fileset.FileMergeReader
	mr        *fileset.MergeReader
	fileCount int
	hash      hash.Hash
}

func newFileReader(file *pfs.File, idx *index.Index, fmr *fileset.FileMergeReader, mr *fileset.MergeReader) *FileReader {
	h := pfs.NewHash()
	for _, dataRef := range idx.DataOp.DataRefs {
		h.Write([]byte(dataRef.Hash))
	}
	return &FileReader{
		file: file,
		idx:  idx,
		fmr:  fmr,
		mr:   mr,
		hash: h,
	}
}

func (fr *FileReader) updateFileInfo(idx *index.Index) {
	fr.fileCount++
	for _, dataRef := range idx.DataOp.DataRefs {
		fr.hash.Write([]byte(dataRef.Hash))
	}
}

// Info returns the info for the file.
func (fr *FileReader) Info() *pfs.FileInfoNewStorage {
	return &pfs.FileInfoNewStorage{
		File: fr.file,
		Hash: pfs.EncodeHash(fr.hash.Sum(nil)),
	}
}

// Get writes a tar stream that contains the file.
func (fr *FileReader) Get(w io.Writer, noPadding ...bool) error {
	if err := fr.fmr.Get(w); err != nil {
		return err
	}
	for fr.fileCount > 0 {
		fmr, err := fr.mr.Next()
		if err != nil {
			return err
		}
		if err := fmr.Get(w); err != nil {
			return err
		}
		fr.fileCount--
	}
	if len(noPadding) > 0 && noPadding[0] {
		return nil
	}
	// Close a tar writer to create tar EOF padding.
	return tar.NewWriter(w).Close()
}

func (fr *FileReader) drain() error {
	for fr.fileCount > 0 {
		if _, err := fr.mr.Next(); err != nil {
			return err
		}
		fr.fileCount--
	}
	return nil
}

// compact is the entrypoint to compaction.
func (d *driver) compact(ctx context.Context, master *work.Master, outputPath string, inputPrefixes []string) error {
	// resolve prefixes into paths
	inputPaths := []string{}
	for _, inputPrefix := range inputPrefixes {
		err := d.storage.WalkFileSets(ctx, inputPrefix, func(inputPath string) error {
			inputPaths = append(inputPaths, inputPath)
			return nil
		})
		if err != nil {
			return err
		}
	}
	if d.env.StorageCompactionMaxFanIn < 2 {
		panic("StorageCompactionMaxFanIn cannot be < 2")
	}
	return d.compactIter(ctx, compactParams{
		master:     master,
		inputPaths: inputPaths,
		outputPath: outputPath,
		maxFanIn:   d.env.StorageCompactionMaxFanIn,
	})
}

type compactParams struct {
	master     *work.Master
	outputPath string
	inputPaths []string
	maxFanIn   int
}

// compactIter is one level of compaction.  It will only perform compaction
// if len(inputPaths) < params.maxFanIn otherwise it will split inputPaths recursively.
func (d *driver) compactIter(ctx context.Context, params compactParams) (retErr error) {
	scratch := path.Join(tmpPrefix, uuid.NewWithoutDashes())
	defer func() {
		if err := d.storage.Delete(ctx, scratch); retErr == nil {
			retErr = err
		}
	}()
	// exceeds maxFanIn need to branch out
	inputPaths := params.inputPaths
	if len(inputPaths) > params.maxFanIn {
		childOutputPaths := []string{}
		childSize := len(params.inputPaths) / params.maxFanIn
		if len(params.inputPaths)%params.maxFanIn != 0 {
			childSize++
		}
		group, ctx := errgroup.WithContext(ctx)
		for i := 0; i < params.maxFanIn; i++ {
			start := i * childSize
			end := (i + 1) * childSize
			if end > len(inputPaths) {
				end = len(inputPaths)
			}
			childOutputPath := path.Join(scratch, strconv.Itoa(i))
			childOutputPaths = append(childOutputPaths, childOutputPath)
			group.Go(func() error {
				return d.compactIter(ctx, compactParams{
					master:     params.master,
					inputPaths: inputPaths[start:end],
					outputPath: childOutputPath,
					maxFanIn:   params.maxFanIn,
				})
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}
		inputPaths = childOutputPaths
	}
	// within maxFanIn, generate compaction tasks for workers
	return d.shardedCompact(ctx, params.master, params.outputPath, inputPaths)
}

// shardedCompact generates shards for the fileset(s) in inputPaths,
// gives those shards to workers, and waits for them to complete
// fan-in: spatially, and temporally to each shard will be len(inputPaths)
// the shards are then serially concatenated, fan-in here is spatially the number of shards.
func (d *driver) shardedCompact(ctx context.Context, master *work.Master, outputPath string, inputPaths []string) (retErr error) {
	scratch := path.Join(tmpPrefix, uuid.NewWithoutDashes())
	defer func() {
		if err := d.storage.Delete(ctx, scratch); retErr == nil {
			retErr = err
		}
	}()
	compaction := &pfs.Compaction{InputPrefixes: inputPaths}
	var subtasks []*work.Task
	var shardOutputs []string
	if err := d.storage.Shard(ctx, inputPaths, func(pathRange *index.PathRange) error {
		shardOutputPath := path.Join(scratch, strconv.Itoa(len(subtasks)))
		shardOutputs = append(shardOutputs, shardOutputPath)
		shard, err := serializeShard(&pfs.Shard{
			Compaction: compaction,
			Range: &pfs.PathRange{
				Lower: pathRange.Lower,
				Upper: pathRange.Upper,
			},
			OutputPath: shardOutputPath,
		})
		if err != nil {
			return err
		}
		subtasks = append(subtasks, &work.Task{Data: shard})
		return nil
	}); err != nil {
		return err
	}
	if err := master.RunSubtasks(subtasks, func(_ context.Context, taskInfo *work.TaskInfo) error {
		if taskInfo.State == work.State_FAILURE {
			return errors.Errorf(taskInfo.Reason)
		}
		return nil
	}); err != nil {
		return err
	}
	return d.concatFileSets(ctx, outputPath, shardOutputs)
}

// concatFileSets concatenates the filesets in inputPaths and writes the result to outputPath
// TODO: move this to the fileset package, and error if the entries are not sorted.
func (d *driver) concatFileSets(ctx context.Context, outputPath string, inputPaths []string) error {
	fsw := d.storage.NewWriter(ctx, outputPath)
	for _, inputPath := range inputPaths {
		fsr := d.storage.NewReader(ctx, inputPath)
		if err := fsr.Iterate(func(fr *fileset.FileReader) error {
			return fsw.CopyFile(fr)
		}); err != nil {
			return err
		}
	}
	return fsw.Close()
}

func serializeShard(shard *pfs.Shard) (*types.Any, error) {
	serializedShard, err := proto.Marshal(shard)
	if err != nil {
		return nil, err
	}
	return &types.Any{
		TypeUrl: "/" + proto.MessageName(shard),
		Value:   serializedShard,
	}, nil
}

func deserializeShard(shardAny *types.Any) (*pfs.Shard, error) {
	shard := &pfs.Shard{}
	if err := types.UnmarshalAny(shardAny, shard); err != nil {
		return nil, err
	}
	return shard, nil
}

func (d *driver) compactionWorker() {
	ctx := context.Background()
	w := work.NewWorker(d.etcdClient, d.prefix, storageTaskNamespace)
	// Configure backoff so we retry indefinitely
	backoffStrat := backoff.NewExponentialBackOff()
	backoffStrat.MaxElapsedTime = 0
	err := backoff.RetryNotify(func() error {
		return w.Run(ctx, func(ctx context.Context, subtask *work.Task) error {
			shard, err := deserializeShard(subtask.Data)
			if err != nil {
				return err
			}
			pathRange := &index.PathRange{
				Lower: shard.Range.Lower,
				Upper: shard.Range.Upper,
			}
			return d.storage.Compact(ctx, shard.OutputPath, shard.Compaction.InputPrefixes, index.WithRange(pathRange))
		})
	}, backoffStrat, func(err error, t time.Duration) error {
		log.Printf("error in compaction worker: %v", err)
		// non-nil shuts down retry loop
		return nil
	})
	// never ending backoff should prevent us from getting here.
	panic(err)
}
