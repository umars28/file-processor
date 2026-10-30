package pipeline

import (
	"cmp"
	"io"
	"slices"
	"sync/atomic"
	"time"
)

const DefaultProgressInterval = 200 * time.Millisecond

const (
	StatusPending FileStatus = "pending"
	StatusRunning FileStatus = "running"
)

type FileProgress struct {
	ID        FileID
	Status    FileStatus
	BytesRead int64
	Size      int64
	Valid     int64
	Invalid   int64
}

func (f FileProgress) Percent() (float64, bool) {
	if f.Size <= 0 {
		return 0, false
	}
	return 100 * float64(min(f.BytesRead, f.Size)) / float64(f.Size), true
}

type Snapshot struct {
	Files      []FileProgress
	FilesDone  int
	FilesTotal int
	BytesRead  int64
	BytesTotal int64
	Valid      int64
	Invalid    int64
	Totals     Totals
	Elapsed    time.Duration
	Final      bool
}

func (s *Snapshot) Percent() (float64, bool) {
	if s.BytesTotal <= 0 {
		return 0, false
	}
	return 100 * float64(s.BytesRead) / float64(s.BytesTotal), true
}

type Progress struct {
	interval time.Duration
	current  atomic.Pointer[Snapshot]
}

func NewProgress(interval time.Duration) *Progress {
	if interval <= 0 {
		interval = DefaultProgressInterval
	}
	return &Progress{interval: interval}
}

func (p *Progress) Snapshot() *Snapshot { return p.current.Load() }

type byteCounter struct {
	r io.Reader
	n *atomic.Int64
}

func (c byteCounter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n.Add(int64(n))
	return n, err
}

type tracker struct {
	progress *Progress
	start    time.Time
	bytes    map[FileID]*atomic.Int64
	sizes    map[FileID]int64
}

func newTracker(p *Progress, sources []Source) *tracker {
	tr := &tracker{
		progress: p,
		start:    time.Now(),
		bytes:    make(map[FileID]*atomic.Int64, len(sources)),
		sizes:    make(map[FileID]int64, len(sources)),
	}
	for _, src := range sources {
		tr.bytes[src.ID] = &atomic.Int64{}
		tr.sizes[src.ID] = src.Size
	}
	return tr
}

func (tr *tracker) ticks() (<-chan time.Time, func()) {
	if tr.progress == nil {
		return nil, func() {}
	}
	t := time.NewTicker(tr.progress.interval)
	return t.C, t.Stop
}

func (tr *tracker) publish(states []*fileState, totals Totals, final bool) {
	if tr.progress == nil {
		return
	}
	s := &Snapshot{
		Files:      make([]FileProgress, 0, len(states)),
		FilesTotal: len(states),
		Totals:     totals,
		Elapsed:    time.Since(tr.start),
		Final:      final,
	}
	for _, f := range states {
		read := tr.bytes[f.result.ID].Load()
		size := tr.sizes[f.result.ID]
		status := f.result.Status
		if !final {
			status = tr.liveStatus(f, read)
		}
		if size > 0 {
			read = min(read, size)
			if final && status != StatusCancelled {
				read = size
			}
		}
		fp := FileProgress{
			ID:        f.result.ID,
			Status:    status,
			BytesRead: read,
			Size:      size,
			Valid:     f.result.Valid,
			Invalid:   f.result.Invalid,
		}
		if size > 0 {
			s.BytesTotal += size
			s.BytesRead += read
		}
		s.Valid += fp.Valid
		s.Invalid += fp.Invalid
		if status != StatusPending && status != StatusRunning {
			s.FilesDone++
		}
		s.Files = append(s.Files, fp)
	}
	slices.SortFunc(s.Files, func(a, b FileProgress) int { return cmp.Compare(a.ID, b.ID) })
	tr.progress.current.Store(s)
}

func (tr *tracker) liveStatus(f *fileState, read int64) FileStatus {
	switch {
	case f.cancelled:
		return StatusCancelled
	case f.done():
		return fileStatus(f.result)
	case f.received > 0 || read > 0:
		return StatusRunning
	default:
		return StatusPending
	}
}
