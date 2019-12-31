package wal

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	// ErrCorrupt is returns when the log is corrupt.
	ErrCorrupt = errors.New("log corrupt")

	// ErrClosed is returned when an operation cannot be completed because
	// the log is closed.
	ErrClosed = errors.New("log closed")

	// ErrNotFound is returned when an entry is not found.
	ErrNotFound = errors.New("not found")

	// ErrOutOfOrder is returned from Write() when the index is not equal to
	// LastIndex()+1. It's required that log monotonically grows by one and has
	// no gaps. Thus, the series 10,11,12,13,14 is valid, but 10,11,13,14 is
	// not because there's a gap between 11 and 13. Also, 10,12,11,13 is not
	// valid because 12 and 11 are out of order.
	ErrOutOfOrder = errors.New("out of order")

	// ErrOutOfRange is returned from TruncateFront() and TruncateBack() when
	// the index not in the range of the log's first and last index. Or, this
	// may be returned when the caller is attempting to remove *all* entries;
	// The log requires that at least one entry exists following a truncate.
	ErrOutOfRange = errors.New("out of range")
)

// Durability policy.
type Durability int8

const (
	// Low durability flushes data to operating system only when the buffer is
	// at capacity. A process crash could cause data loss.
	Low Durability = -1
	// Medium durability flushes data to the operating system after every
	// new entry is addded to the log. A server crash could cause data loss.
	Medium Durability = 0
	// High durability syncs data to disk after every new entry is added to
	// the log. All entries are persisted to disk and crashes will not cause
	// data loss.
	High Durability = 1
)

// LogFormat is the format of the log files.
type LogFormat byte

const (
	// Binary format writes entries in binary. This is the default and, unless
	// a good reason otherwise, should be used in production.
	Binary LogFormat = 0
	// JSON format writes entries as JSON lines. This causes larger, human
	// readable files.
	JSON LogFormat = 1
)

// Options for Log
type Options struct {
	// Durability policy. Default is High.
	Durability Durability
	// SegmentSize of each segment. This is just a target value, actual size
	// may differ. Default is 500 MB.
	SegmentSize int
	// LogFormat is the format of the log files. Default is Binary.
	LogFormat LogFormat
}

// DefaultOptions for Open().
var DefaultOptions = &Options{
	Durability:  High,     // Fsync after every write
	SegmentSize: 52428800, // 50 MB log segment files.
	LogFormat:   Binary,   // Binary format is small and fast.
}

const maxReaders = 8 // maximum number of opened readers.

// Log represents a write ahead log
type Log struct {
	path       string    // absolute path to log directory
	opts       Options   // log options
	closed     bool      // log is closed
	segments   []segment // all known log segments
	firstIndex uint64    // index of the first entry in log
	lastIndex  uint64    // index of the last entry in log
	file       *os.File  // tail segment file handle
	buffer     []byte    // tail segment file write buffer
	fileSize   int       // tail segment file size, including buffer
	readers    []*reader // all opened readers
}

// segment represents a single segment file.
type segment struct {
	path  string // path of segment file
	index uint64 // first index of segment
}

type reader struct {
	sindex int           // segment index
	nindex uint64        // next entry index
	file   *os.File      // opened file
	rd     *bufio.Reader // reader
}

// Open a new write ahead log
func Open(path string, opts *Options) (*Log, error) {
	if opts == nil {
		opts = DefaultOptions
	}
	var err error
	path, err = abs(path)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, opts: *opts}
	_ = os.MkdirAll(path, 0777)
	if err := l.load(); err != nil {
		return nil, err
	}
	return l, nil
}

func abs(path string) (string, error) {
	if path == ":memory:" {
		return "", errors.New("in-memory log not supported")
	}
	return filepath.Abs(path)
}

// load all the segments. This operation also cleansup any START/END segments.
func (l *Log) load() error {
	fis, err := ioutil.ReadDir(l.path)
	if err != nil {
		return err
	}
	startIdx := -1
	endIdx := -1
	for _, fi := range fis {
		name := fi.Name()
		if fi.IsDir() || len(name) < 20 {
			continue
		}
		index, err := strconv.ParseUint(name[:20], 10, 64)
		if err != nil || index == 0 {
			continue
		}
		isStart := len(name) == 26 && strings.HasSuffix(name, ".START")
		isEnd := len(name) == 24 && strings.HasSuffix(name, ".END")
		if len(name) == 20 || isStart || isEnd {
			if isStart {
				startIdx = len(l.segments)
			} else if isEnd && endIdx == -1 {
				endIdx = len(l.segments)
			}
			l.segments = append(l.segments, segment{
				index: index,
				path:  filepath.Join(l.path, name),
			})
		}
	}
	if len(l.segments) == 0 {
		// Create a new log
		l.segments = append(l.segments, segment{
			index: 1,
			path:  filepath.Join(l.path, segmentName(1)),
		})
		l.firstIndex = 1
		l.lastIndex = 0
		l.file, err = os.Create(l.segments[0].path)
		return err
	}
	// Open existing log. Clean up log if START of END segments exists.
	if startIdx != -1 {
		if endIdx != -1 {
			// There should not be a START and END at the same time
			return ErrCorrupt
		}
		// Delete all files leading up to START
		for i := 0; i < startIdx; i++ {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]segment{}, l.segments[startIdx:]...)
		// Rename the START segment
		orgPath := l.segments[0].path
		finalPath := orgPath[:len(orgPath)-len(".START")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[0].path = finalPath
	}
	if endIdx != -1 {
		// Delete all files following END
		for i := len(l.segments) - 1; i > endIdx; i-- {
			if err := os.Remove(l.segments[i].path); err != nil {
				return err
			}
		}
		l.segments = append([]segment{}, l.segments[:endIdx+1]...)
		if len(l.segments) > 1 && l.segments[len(l.segments)-2].index ==
			l.segments[len(l.segments)-1].index {
			// remove the segment prior to the END segment because it shares
			// the same starting index.
			l.segments[len(l.segments)-2] = l.segments[len(l.segments)-1]
			l.segments = l.segments[:len(l.segments)-1]
		}
		// Rename the END segment
		orgPath := l.segments[len(l.segments)-1].path
		finalPath := orgPath[:len(orgPath)-len(".END")]
		err := os.Rename(orgPath, finalPath)
		if err != nil {
			return err
		}
		l.segments[len(l.segments)-1].path = finalPath
	}
	// Open the last segment for appending
	l.firstIndex = l.segments[0].index
	l.file, err = os.OpenFile(l.segments[len(l.segments)-1].path,
		os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	// Read the last segment to the end of the log
	rd := bufio.NewReader(l.file)
	for {
		idx, _, err := readEntry(rd, l.opts.LogFormat, true)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		l.lastIndex = idx
	}
	n, err := l.file.Seek(0, 2)
	if err != nil {
		return err
	}
	l.fileSize = int(n)
	return nil
}

func must(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

// segmentName returns a 20-byte textual representation of an index
// for lexical ordering. This is used for the file names of log segments.
func segmentName(index uint64) string {
	return fmt.Sprintf("%020d", index)
}

// Close the log
func (l *Log) Close() error {
	if l.closed {
		return ErrClosed
	}
	l.flush()
	must(nil, l.file.Close())
	for len(l.readers) > 0 {
		l.closeReader(l.readers[0])
	}
	l.readers = nil
	l.segments = nil
	l.closed = true
	return nil
}

func (l *Log) flush() {
	if len(l.buffer) > 0 {
		must(l.file.Write(l.buffer))
		l.buffer = l.buffer[:0]
		if l.opts.Durability >= High {
			must(nil, l.file.Sync())
		}
	}
}

// Write an entry to the log.
func (l *Log) Write(index uint64, data []byte) error {
	if l.closed {
		return ErrClosed
	}
	if index != l.lastIndex+1 {
		return ErrOutOfOrder
	}
	if l.fileSize >= l.opts.SegmentSize {
		l.cycle()
	}
	l.appendEntry(index, data)
	if l.opts.Durability >= Medium || len(l.buffer) >= 4096 {
		l.flush()
	}
	l.lastIndex = index
	return nil
}

// Cycle the old segment for a new segment.
func (l *Log) cycle() {
	l.flush()
	must(nil, l.file.Close())
	s := segment{
		index: l.lastIndex + 1,
		path:  filepath.Join(l.path, segmentName(l.lastIndex+1)),
	}
	l.file = must(os.Create(s.path)).(*os.File)
	l.fileSize = 0
	l.segments = append(l.segments, s)
	l.buffer = nil
}

// appendEntry to the log buffer. This also increases the log segment fileSize.
func (l *Log) appendEntry(index uint64, data []byte) {
	mark := len(l.buffer)
	if l.opts.LogFormat == JSON {
		l.buffer = appendJSONEntry(l.buffer, index, data)
	} else {
		l.buffer = appendBinaryEntry(l.buffer, index, data)
	}
	l.fileSize += len(l.buffer) - mark

}

func appendJSONEntry(dst []byte, index uint64, data []byte) []byte {
	// {"index":number,"data":string}
	dst = append(dst, `{"index":"`...)
	dst = strconv.AppendUint(dst, index, 10)
	dst = append(dst, `","data":`...)
	dst = appendJSONString(dst, data)
	dst = append(dst, '}', '\n')
	return dst
}

func appendJSONString(dst []byte, s []byte) []byte {
	for i := 0; i < len(s); i++ {
		if s[i] < ' ' || s[i] == '\\' || s[i] == '"' || s[i] > 126 {
			d, _ := json.Marshal(string(s))
			return append(dst, d...)
		}
	}
	dst = append(dst, '"')
	dst = append(dst, s...)
	dst = append(dst, '"')
	return dst
}

func appendBinaryEntry(dst []byte, index uint64, data []byte) []byte {
	// index + data_size + data
	dst = appendUvarint(dst, index)
	dst = appendUvarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	return dst
}

func appendUvarint(dst []byte, x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	dst = append(dst, buf[:n]...)
	return dst
}

// Batch of entries. Used to write multiple entries at once using WriteBatch().
type Batch struct {
	indexes   []uint64
	dataSizes []int
	datas     []byte
}

// Write an entry to the batch
func (b *Batch) Write(index uint64, data []byte) {
	b.indexes = append(b.indexes, index)
	b.dataSizes = append(b.dataSizes, len(data))
	b.datas = append(b.datas, data...)
}

// Clear the batch for reuse.
func (b *Batch) Clear() {
	b.indexes = b.indexes[:0]
	b.dataSizes = b.dataSizes[:0]
	b.datas = b.datas[:0]
}

// WriteBatch writes the entries in the batch to the log in the order that they
// were added to the batch. The batch is cleared upon a successful return.
func (l *Log) WriteBatch(b *Batch) error {
	if l.closed {
		return ErrClosed
	}
	if len(b.indexes) == 0 {
		return nil
	}
	// check all indexes in batch
	for i := 0; i < len(b.indexes); i++ {
		if b.indexes[i] != l.lastIndex+uint64(i+1) {
			return ErrOutOfOrder
		}
	}
	if l.fileSize >= l.opts.SegmentSize {
		l.cycle()
	}
	datas := b.datas
	for i := 0; i < len(b.indexes); i++ {
		data := datas[:b.dataSizes[i]]
		l.appendEntry(b.indexes[i], data)
		datas = datas[b.dataSizes[i]:]
	}
	if l.opts.Durability >= Medium || len(l.buffer) >= 4096 {
		l.flush()
	}
	l.lastIndex = b.indexes[len(b.indexes)-1]
	b.Clear()
	return nil
}

// FirstIndex returns the index of the first entry in the log. Returns zero
// when log has no entries.
func (l *Log) FirstIndex() (index uint64, err error) {
	if l.closed {
		return 0, ErrClosed
	}
	// We check the lastIndex for zero because the firstIndex is always one or
	// more, even when there's no entries
	if l.lastIndex == 0 {
		return 0, nil
	}
	return l.firstIndex, nil
}

// LastIndex returns the index of the last entry in the log. Returns zero when
// log has no entries.
func (l *Log) LastIndex() (index uint64, err error) {
	if l.closed {
		return 0, ErrClosed
	}
	if l.lastIndex == 0 {
		return 0, nil
	}
	return l.lastIndex, nil
}

// findSegment performs a bsearch on the segments
func (l *Log) findSegment(index uint64) int {
	i, j := 0, len(l.segments)
	for i < j {
		h := i + (j-i)/2
		if index >= l.segments[h].index {
			i = h + 1
		} else {
			j = h
		}
	}
	return i - 1
}

// Read an entry from the log. This function reads an entry from disk and is
// optimized for sequential reads. Randomly accessing entries is slow.
func (l *Log) Read(index uint64) (data []byte, err error) {
	if l.closed {
		return nil, ErrClosed
	}
	if index == 0 || index < l.firstIndex || index > l.lastIndex {
		return nil, ErrNotFound
	}

	// find an opened reader
	var r *reader
	for _, or := range l.readers {
		if or.nindex == index {
			r = or
			break
		}
	}
	if r == nil {
		// Reader not found, open a new reader and return the entry at index
		return l.openReader(index)
	}

	// Read next entry from reader
	for {
		eindex, edata, err := l.readEntry(r)
		if err == io.EOF {
			// Reached the end of the segment.
			if r.sindex == len(l.segments)-1 {
				// At the end of the last segment file.
				if len(l.buffer) > 0 {
					// But the segment has a buffer, flush it and try again
					must(l.file.Write(l.buffer))
					l.buffer = l.buffer[:0]
					continue
				}
				// Log is missing some final entries, consider is corrput
				l.closeReader(r)
				return nil, ErrCorrupt
			}
			// Close the old reader and open a new one
			l.closeReader(r)
			return l.openReader(index)
		}
		if eindex != index {
			// Log has gaps or is out of order, corrupt file.
			l.closeReader(r)
			return nil, ErrCorrupt
		}
		r.nindex++
		if r.nindex == l.lastIndex+1 {
			// read the last entry, close the reader
			l.closeReader(r)
		}
		return edata, nil
	}
}

// openReader opens a new reader and returns the data belong to entry at index.
func (l *Log) openReader(index uint64) (data []byte, err error) {
	// reader not found, open a new one
	r := &reader{}
	r.sindex = l.findSegment(index)
	r.nindex = l.segments[r.sindex].index
	r.file, err = os.Open(l.segments[r.sindex].path)
	if err != nil {
		return nil, err
	}
	r.rd = bufio.NewReader(r.file)
	if r.sindex == len(l.segments)-1 {
		if len(l.buffer) > 0 {
			// Reading from the last segment, which has a buffer, flush the
			// buffer before reading from file.
			must(l.file.Write(l.buffer))
			l.buffer = l.buffer[:0]
		}
	}
	// Scan the file for the entry at index.
	for {
		eindex, edata, err := l.readEntry(r)
		if err != nil {
			// Bad news. Likely a corrupt file.
			r.file.Close()
			return nil, err
		}
		if eindex != r.nindex {
			// Log has gaps or is out of order, corrupt file.
			r.file.Close()
			return nil, ErrCorrupt
		}
		r.nindex = eindex + 1
		if eindex == index {
			// Add the new reader to the front of the list of opened readers.
			l.readers = append(l.readers, nil)
			copy(l.readers[1:], l.readers[:len(l.readers)-1])
			l.readers[0] = r
			for len(l.readers) > maxReaders {
				// remove the oldest readers
				l.closeReader(l.readers[len(l.readers)-1])
			}
			return edata, nil
		}
	}
}

// closeReader closes the reader and removes it from the list of opened readers.
func (l *Log) closeReader(r *reader) {
	for i, rr := range l.readers {
		if rr == r {
			must(nil, r.file.Close())
			l.readers[i] = l.readers[len(l.readers)-1]
			l.readers[len(l.readers)-1] = nil
			l.readers = l.readers[:len(l.readers)-1]
			return
		}
	}
}

// readEntry reads the next entry from reader. Returns io.EOF if the readers
// has reached the end of a segment.
func (l *Log) readEntry(r *reader) (index uint64, data []byte, err error) {
	return readEntry(r.rd, l.opts.LogFormat, false)
}

// readEntry from bufio.Reader. The discardData param will discard the entry
// data ane return nil.
func readEntry(rd *bufio.Reader, frmt LogFormat, discardData bool) (
	index uint64, data []byte, err error,
) {
	if frmt == JSON {
		line, err := rd.ReadBytes('\n')
		if err != nil {
			if err == io.EOF && len(line) > 0 {
				return 0, nil, ErrCorrupt
			}
			return 0, nil, err
		}
		var m map[string]string
		if err := json.Unmarshal(line, &m); err != nil {
			return 0, nil, ErrCorrupt
		}
		index, err = strconv.ParseUint(m["index"], 10, 64)
		if err != nil {
			return 0, nil, ErrCorrupt
		}
		s, ok := m["data"]
		if !ok {
			return 0, nil, ErrCorrupt
		}
		if !discardData {
			data = []byte(s)
		}
		return index, data, nil
	}
	index, err = binary.ReadUvarint(rd)
	if err != nil {
		return 0, nil, err
	}
	dataSize, err := binary.ReadUvarint(rd)
	if err != nil {
		if err == io.EOF {
			return 0, nil, ErrCorrupt
		}
		return 0, nil, err
	}
	if discardData {
		_, err = rd.Discard(int(dataSize))
	} else {
		data = make([]byte, dataSize)
		_, err = io.ReadFull(rd, data)
	}
	if err != nil {
		if err == io.EOF {
			return 0, nil, ErrCorrupt
		}
		return 0, nil, err
	}
	return index, data, nil
}

// TruncateFront truncates the front of the log by removing all entries that
// are before the provided `firstIndex`. In other words the entry at
// `firstIndex` becomes the first entry in the log.
func (l *Log) TruncateFront(firstIndex uint64) error {
	if l.closed {
		return ErrClosed
	}

	index := firstIndex // alias

	if index == 0 || l.lastIndex == 0 ||
		index < l.firstIndex || index > l.lastIndex {
		return ErrOutOfRange
	}

	// Flush the buffer, if needed.
	if len(l.buffer) > 0 {
		must(l.file.Write(l.buffer))
		l.buffer = nil
	}
	// Close all readers
	for len(l.readers) > 0 {
		l.closeReader(l.readers[0])
	}

	if index == l.firstIndex {
		// nothing to truncate
		return nil
	}

	// Find which segment has the entry with the index.
	sidx := l.findSegment(index)
	// Find the file offset of the first byte belonging to the entry at index.
	f, err := os.Open(l.segments[sidx].path)
	if err != nil {
		return err
	}
	defer f.Close()
	if index > l.segments[sidx].index {
		// Read all entries prior to entry at index.
		rd := bufio.NewReader(f)
		var found bool
		for {
			ridx, _, err := readEntry(rd, l.opts.LogFormat, true)
			if err != nil {
				return err
			}
			if ridx == index-1 {
				// Seek to exact position of entry, the reader likely overread
				// the file, so we need to back up what has been buffered.
				if _, err := f.Seek(0-int64(rd.Buffered()), 1); err != nil {
					return err
				}
				found = true
				break
			}
		}
		if !found {
			return ErrCorrupt
		}
	}
	// Create a temp file in the same log directory and copy all of the data
	// starting at the seeked offset position.
	tempName := filepath.Join(l.path, "TEMP")
	ftmp, err := os.Create(tempName)
	if err != nil {
		return err
	}
	defer func() {
		ftmp.Close()
		os.Remove(tempName)
	}()
	if _, err := io.Copy(ftmp, f); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ftmp.Close(); err != nil {
		return err
	}
	// Then rename the TEMP file to it's START file name.
	startName := filepath.Join(l.path, segmentName(index)+".START")
	if err := os.Rename(tempName, startName); err != nil {
		return err
	}
	// All operations from this point on are syscalls and must succeed,
	// otherwise we panic.
	// Remove all of the unneeded segments.
	for i := 0; i <= sidx; i++ {
		must(nil, os.Remove(l.segments[i].path))
	}
	// Remove the ".START" suffix from the file name.
	finalName := startName[:len(startName)-len(".START")]
	must(nil, os.Rename(startName, finalName))
	// Modify the segments list.
	l.segments = append([]segment{segment{path: finalName, index: index}},
		l.segments[sidx+1:]...)
	if len(l.segments) == 1 {
		// The last segment has been changed which means we need to reopen the
		// appendable file and seek to the end.
		must(nil, l.file.Close())
		l.file = must(os.OpenFile(finalName, os.O_RDWR, 0666)).(*os.File)
		l.fileSize = int(must(l.file.Seek(0, 2)).(int64))
		l.buffer = nil
	}
	l.firstIndex = index
	return nil
}

// TruncateBack truncates the back of the log by removing all entries that
// are after the provided `lastIndex`. In other words the entry at `lastIndex`
// becomes the last entry in the log.
func (l *Log) TruncateBack(lastIndex uint64) error {
	if l.closed {
		return ErrClosed
	}

	index := lastIndex // alias

	if index == 0 || l.lastIndex == 0 ||
		index > l.lastIndex || index < l.firstIndex {
		return ErrOutOfRange
	}

	// Flush the buffer, if needed.
	if len(l.buffer) > 0 {
		must(l.file.Write(l.buffer))
		l.buffer = nil
	}
	// Close all readers
	for len(l.readers) > 0 {
		l.closeReader(l.readers[0])
	}

	if index == l.lastIndex {
		// nothing to truncate
		return nil
	}

	// Find which segment has the entry with the index.
	sidx := l.findSegment(index)
	// Find the file offset of the first byte belonging to the entry at index.
	f, err := os.Open(l.segments[sidx].path)
	if err != nil {
		return err
	}
	defer f.Close()
	// Read all entries prior to entry at index.
	rd := bufio.NewReader(f)
	var found bool
	var offset int64
	for {
		ridx, _, err := readEntry(rd, l.opts.LogFormat, true)
		if err != nil {
			return err
		}
		if ridx == index {
			// Seek to exact position of entry, the reader likely overread
			// the file, so we need to back up what has been buffered.
			offset, err = f.Seek(0-int64(rd.Buffered()), 1)
			if err != nil {
				return err
			}
			found = true
			// Rewind the segment
			if _, err := f.Seek(0, 0); err != nil {
				return err
			}
			break
		}
	}
	if !found {
		return ErrCorrupt
	}
	// Create a temp file in the same log directory and copy all of the data
	// up to the offset.
	tempName := filepath.Join(l.path, "TEMP")
	ftmp, err := os.Create(tempName)
	if err != nil {
		return err
	}
	defer func() {
		ftmp.Close()
		os.Remove(tempName)
	}()
	if _, err := io.CopyN(ftmp, f, offset); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := ftmp.Close(); err != nil {
		return err
	}
	// Then rename the TEMP file to it's END file name.
	endName := l.segments[sidx].path + ".END"
	if err := os.Rename(tempName, endName); err != nil {
		return err
	}
	// All operations from this point on are syscalls and must succeed,
	// otherwise we panic.
	// Remove all of the unneeded segments.
	for i := len(l.segments) - 1; i >= sidx; i-- {
		must(nil, os.Remove(l.segments[i].path))
	}
	// Modify the segments list.
	l.segments = append([]segment{}, l.segments[:sidx+1]...)
	// Remove the ".END" suffix from the file name.
	finalName := endName[:len(endName)-len(".END")]
	must(nil, os.Rename(endName, finalName))
	// The last segment has been changed which means we need to reopen the
	// appendable file and seek to the end.
	must(nil, l.file.Close())
	l.file = must(os.OpenFile(finalName, os.O_RDWR, 0666)).(*os.File)
	l.fileSize = int(must(l.file.Seek(0, 2)).(int64))
	l.buffer = nil
	l.lastIndex = index
	return nil
}
