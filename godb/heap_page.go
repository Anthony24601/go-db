package godb

import (
	"bytes"
	"encoding/binary"
	"sync"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	tupleDesc  TupleDesc
	totalSlots int32
	slotsUsed  int32
	dirtyFlag  bool
	tuples     []*Tuple
	pageNumber int
	heapFile   *HeapFile
	sync.Mutex
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNumber int, f *HeapFile) (*heapPage, error) {
	var page_slots = int32((PageSize - 8) / desc.bytesPerTuple())
	var newTuples = make([]*Tuple, page_slots)
	var newPage = heapPage{*desc, page_slots, 0, false, newTuples, pageNumber, f, sync.Mutex{}}
	return &newPage, nil
}

// Hint: heapfile/insertTuple needs function there:  func (h *heapPage) getNumEmptySlots() int
func (h *heapPage) getNumEmptySlots() int {
	var slots = int(h.totalSlots - h.slotsUsed)
	return slots
}

func (h *heapPage) getNumSlots() int {
	return int(h.totalSlots)
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {

	for i := 0; i < h.getNumSlots(); i += 1 {
		if h.tuples[i] == nil {
			h.tuples[i] = t
			h.slotsUsed += 1
			t.Rid = heapFileRid{h.pageNumber, i}
			return t.Rid, nil
		}
	}

	return 0, GoDBError{PageFullError, "No more Free slots!"}
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	var slot = rid.(heapFileRid).slotNumber
	if slot < 0 || slot >= int(h.totalSlots) {
		return GoDBError{TupleNotFoundError, "Outside of range, not a slot possible to delete"}
	} else if h.tuples[slot] == nil {
		return GoDBError{TupleNotFoundError, "No element found in slot"}
	}
	h.slotsUsed--
	h.tuples[slot] = nil
	return nil
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirtyFlag
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.dirtyFlag = dirty // Why do we need transactionID here? // ASK!
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.heapFile
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	var new_buffer = new(bytes.Buffer)

	// Header stuff
	err := binary.Write(new_buffer, binary.LittleEndian, (int32)(h.totalSlots))
	if err != nil {
		return nil, err
	}
	err = binary.Write(new_buffer, binary.LittleEndian, (int32)(h.slotsUsed))
	if err != nil {
		return nil, err
	}

	// Tuples
	for i := 0; i < int(h.totalSlots); i += 1 {
		if h.tuples[i] != nil {
			err = h.tuples[i].writeTo(new_buffer)
			if err != nil {
				return nil, err
			}
		}
	}
	new_buffer.Write(make([]byte, PageSize-new_buffer.Len())) // Padding!
	return new_buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// Grab header information first
	var totalSlots int32
	err := binary.Read(buf, binary.LittleEndian, &totalSlots)
	if err != nil {
		return err
	}
	h.totalSlots = totalSlots

	var slotsUsed int32
	err = binary.Read(buf, binary.LittleEndian, &slotsUsed)
	if err != nil {
		return err
	}
	h.slotsUsed = slotsUsed

	// Read in tuples
	var tuples = make([]*Tuple, totalSlots)
	for i := 0; i < int(slotsUsed); i += 1 {
		tuple, err := readTupleFrom(buf, &h.tupleDesc)
		if err != nil {
			return err
		}
		tuple.Rid = heapFileRid{h.pageNumber, i}
		tuples[i] = tuple
	}
	h.tuples = tuples
	h.dirtyFlag = false
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	return func() (*Tuple, error) {
		for i := 0; i < int(p.totalSlots); i += 1 {
			if p.tuples[i] != nil {
				return p.tuples[i], nil
			}
		}
		return nil, nil
	}
}
