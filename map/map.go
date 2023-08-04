// Copyright 2020 Joshua J Baker. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
package btree

import "sync/atomic"

type ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 | ~string
}

type copier[T any] interface {
	Copy() T
}

type isoCopier[T any] interface {
	IsoCopy() T
}

func degreeToMinMax(deg int) (min, max int) {
	if deg <= 0 {
		deg = 32
	} else if deg == 1 {
		deg = 2 // must have at least 2
	}
	max = deg*2 - 1 // max items per node. max children is +1
	min = max / 2
	return min, max
}

var gisoid uint64

func newIsoID() uint64 {
	return atomic.AddUint64(&gisoid, 1)
}

type mapPair[K ordered, V any] struct {
	// The `Value` field should be before the `key` field because doing so
	// allows for the Go compiler to optimize away the `Value` field when
	// it's a `struct{}`, which is the case for `btree.Set`.
	Value V
	Key   K
}

type Map[K ordered, V any] struct {
	Isoid         uint64
	Root          *mapNode[K, V]
	Count         int
	Empty         mapPair[K, V]
	Minn          int // min items
	Maxx          int // max items
	CopyValues    bool
	IsoCopyValues bool
}

func NewMap[K ordered, V any](degree int) *Map[K, V] {
	m := new(Map[K, V])
	m.init(degree)
	return m
}

type mapNode[K ordered, V any] struct {
	Isoid    uint64
	Count    int
	Items    []mapPair[K, V]
	Children *[]*mapNode[K, V]
}

// Copy the node for safe isolation.
func (tr *Map[K, V]) copy(n *mapNode[K, V]) *mapNode[K, V] {
	n2 := new(mapNode[K, V])
	n2.Isoid = tr.Isoid
	n2.Count = n.Count
	n2.Items = make([]mapPair[K, V], len(n.Items), cap(n.Items))
	copy(n2.Items, n.Items)
	if tr.CopyValues {
		for i := 0; i < len(n2.Items); i++ {
			n2.Items[i].Value =
				((interface{})(n2.Items[i].Value)).(copier[V]).Copy()
		}
	} else if tr.IsoCopyValues {
		for i := 0; i < len(n2.Items); i++ {
			n2.Items[i].Value =
				((interface{})(n2.Items[i].Value)).(isoCopier[V]).IsoCopy()
		}
	}
	if !n.leaf() {
		n2.Children = new([]*mapNode[K, V])
		*n2.Children = make([]*mapNode[K, V], len(*n.Children), tr.Maxx+1)
		copy(*n2.Children, *n.Children)
	}
	return n2
}

// isoLoad loads the provided node and, if needed, performs a copy-on-write.
func (tr *Map[K, V]) isoLoad(cn **mapNode[K, V], mut bool) *mapNode[K, V] {
	if mut && (*cn).Isoid != tr.Isoid {
		*cn = tr.copy(*cn)
	}
	return *cn
}

func (tr *Map[K, V]) Copy() *Map[K, V] {
	return tr.IsoCopy()
}

func (tr *Map[K, V]) IsoCopy() *Map[K, V] {
	tr2 := new(Map[K, V])
	*tr2 = *tr
	tr2.Isoid = newIsoID()
	tr.Isoid = newIsoID()
	return tr2
}

func (tr *Map[K, V]) newNode(leaf bool) *mapNode[K, V] {
	n := new(mapNode[K, V])
	n.Isoid = tr.Isoid
	if !leaf {
		n.Children = new([]*mapNode[K, V])
	}
	return n
}

// leaf returns true if the node is a leaf.
func (n *mapNode[K, V]) leaf() bool {
	return n.Children == nil
}

func (tr *Map[K, V]) search(n *mapNode[K, V], key K) (index int, found bool) {
	low, high := 0, len(n.Items)
	for low < high {
		h := (low + high) / 2
		if !(key < n.Items[h].Key) {
			low = h + 1
		} else {
			high = h
		}
	}
	if low > 0 && !(n.Items[low-1].Key < key) {
		return low - 1, true
	}
	return low, false
}

func (tr *Map[K, V]) init(degree int) {
	if tr.Minn != 0 {
		return
	}
	tr.Minn, tr.Maxx = degreeToMinMax(degree)
	_, tr.CopyValues = ((interface{})(tr.Empty.Value)).(copier[V])
	if !tr.CopyValues {
		_, tr.IsoCopyValues = ((interface{})(tr.Empty.Value)).(isoCopier[V])
	}
}

// Set or replace a value for a key
func (tr *Map[K, V]) Set(key K, value V) (V, bool) {
	item := mapPair[K, V]{Key: key, Value: value}
	if tr.Root == nil {
		tr.init(0)
		tr.Root = tr.newNode(true)
		tr.Root.Items = append([]mapPair[K, V]{}, item)
		tr.Root.Count = 1
		tr.Count = 1
		return tr.Empty.Value, false
	}
	prev, replaced, split := tr.nodeSet(&tr.Root, item)
	if split {
		left := tr.Root
		right, median := tr.nodeSplit(left)
		tr.Root = tr.newNode(false)
		*tr.Root.Children = make([]*mapNode[K, V], 0, tr.Maxx+1)
		*tr.Root.Children = append([]*mapNode[K, V]{}, left, right)
		tr.Root.Items = append([]mapPair[K, V]{}, median)
		tr.Root.updateCount()
		return tr.Set(item.Key, item.Value)
	}
	if replaced {
		return prev, true
	}
	tr.Count++
	return tr.Empty.Value, false
}

func (tr *Map[K, V]) nodeSplit(n *mapNode[K, V],
) (right *mapNode[K, V], median mapPair[K, V]) {
	i := tr.Maxx / 2
	median = n.Items[i]

	// right node
	right = tr.newNode(n.leaf())
	right.Items = n.Items[i+1:]
	if !n.leaf() {
		*right.Children = (*n.Children)[i+1:]
	}
	right.updateCount()

	// left node
	n.Items[i] = tr.Empty
	n.Items = n.Items[:i:i]
	if !n.leaf() {
		*n.Children = (*n.Children)[: i+1 : i+1]
	}
	n.updateCount()
	return right, median
}

func (n *mapNode[K, V]) updateCount() {
	n.Count = len(n.Items)
	if !n.leaf() {
		for i := 0; i < len(*n.Children); i++ {
			n.Count += (*n.Children)[i].Count
		}
	}
}

func (tr *Map[K, V]) nodeSet(pn **mapNode[K, V], item mapPair[K, V],
) (prev V, replaced bool, split bool) {
	n := tr.isoLoad(pn, true)
	i, found := tr.search(n, item.Key)
	if found {
		prev = n.Items[i].Value
		n.Items[i] = item
		return prev, true, false
	}
	if n.leaf() {
		if len(n.Items) == tr.Maxx {
			return tr.Empty.Value, false, true
		}
		n.Items = append(n.Items, tr.Empty)
		copy(n.Items[i+1:], n.Items[i:])
		n.Items[i] = item
		n.Count++
		return tr.Empty.Value, false, false
	}
	prev, replaced, split = tr.nodeSet(&(*n.Children)[i], item)
	if split {
		if len(n.Items) == tr.Maxx {
			return tr.Empty.Value, false, true
		}
		right, median := tr.nodeSplit((*n.Children)[i])
		*n.Children = append(*n.Children, nil)
		copy((*n.Children)[i+1:], (*n.Children)[i:])
		(*n.Children)[i+1] = right
		n.Items = append(n.Items, tr.Empty)
		copy(n.Items[i+1:], n.Items[i:])
		n.Items[i] = median
		return tr.nodeSet(&n, item)
	}
	if !replaced {
		n.Count++
	}
	return prev, replaced, false
}

func (tr *Map[K, V]) Scan(iter func(key K, value V) bool) {
	tr.scan(iter, false)
}

func (tr *Map[K, V]) ScanMut(iter func(key K, value V) bool) {
	tr.scan(iter, true)
}

func (tr *Map[K, V]) scan(iter func(key K, value V) bool, mut bool) {
	if tr.Root == nil {
		return
	}
	tr.nodeScan(&tr.Root, iter, mut)
}

func (tr *Map[K, V]) nodeScan(cn **mapNode[K, V],
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.Items); i++ {
			if !iter(n.Items[i].Key, n.Items[i].Value) {
				return false
			}
		}
		return true
	}
	for i := 0; i < len(n.Items); i++ {
		if !tr.nodeScan(&(*n.Children)[i], iter, mut) {
			return false
		}
		if !iter(n.Items[i].Key, n.Items[i].Value) {
			return false
		}
	}
	return tr.nodeScan(&(*n.Children)[len(*n.Children)-1], iter, mut)
}

// Get a value for key.
func (tr *Map[K, V]) Get(key K) (V, bool) {
	return tr.get(key, false)
}

// GetMut gets a value for key.
// If needed, this may perform a copy the resulting value before returning.
//
// Mut methods are only useful when all of the following are true:
//   - The interior data of the value requires changes.
//   - The value is a pointer type.
//   - The BTree has been copied using `Copy()` or `IsoCopy()`.
//   - The value itself has a `Copy()` or `IsoCopy()` method.
//
// Mut methods may modify the tree structure and should have the same
// considerations as other mutable operations like Set, Delete, Clear, etc.
func (tr *Map[K, V]) GetMut(key K) (V, bool) {
	return tr.get(key, true)
}

func (tr *Map[K, V]) get(key K, mut bool) (V, bool) {
	if tr.Root == nil {
		return tr.Empty.Value, false
	}
	n := tr.isoLoad(&tr.Root, mut)
	for {
		i, found := tr.search(n, key)
		if found {
			return n.Items[i].Value, true
		}
		if n.leaf() {
			return tr.Empty.Value, false
		}
		n = tr.isoLoad(&(*n.Children)[i], mut)
	}
}

// Len returns the number of items in the tree
func (tr *Map[K, V]) Len() int {
	return tr.Count
}

// Delete a value for a key and returns the deleted value.
// Returns false if there was no value by that key found.
func (tr *Map[K, V]) Delete(key K) (V, bool) {
	if tr.Root == nil {
		return tr.Empty.Value, false
	}
	prev, deleted := tr.delete(&tr.Root, false, key)
	if !deleted {
		return tr.Empty.Value, false
	}
	if len(tr.Root.Items) == 0 && !tr.Root.leaf() {
		tr.Root = (*tr.Root.Children)[0]
	}
	tr.Count--
	if tr.Count == 0 {
		tr.Root = nil
	}
	return prev.Value, true
}

func (tr *Map[K, V]) delete(pn **mapNode[K, V], max bool, key K,
) (mapPair[K, V], bool) {
	n := tr.isoLoad(pn, true)
	var i int
	var found bool
	if max {
		i, found = len(n.Items)-1, true
	} else {
		i, found = tr.search(n, key)
	}
	if n.leaf() {
		if found {
			// found the items at the leaf, remove it and return.
			prev := n.Items[i]
			copy(n.Items[i:], n.Items[i+1:])
			n.Items[len(n.Items)-1] = tr.Empty
			n.Items = n.Items[:len(n.Items)-1]
			n.Count--
			return prev, true
		}
		return tr.Empty, false
	}

	var prev mapPair[K, V]
	var deleted bool
	if found {
		if max {
			i++
			prev, deleted = tr.delete(&(*n.Children)[i], true, tr.Empty.Key)
		} else {
			prev = n.Items[i]
			maxItem, _ := tr.delete(&(*n.Children)[i], true, tr.Empty.Key)
			deleted = true
			n.Items[i] = maxItem
		}
	} else {
		prev, deleted = tr.delete(&(*n.Children)[i], max, key)
	}
	if !deleted {
		return tr.Empty, false
	}
	n.Count--
	if len((*n.Children)[i].Items) < tr.Minn {
		tr.nodeRebalance(n, i)
	}
	return prev, true
}

// nodeRebalance rebalances the child nodes following a delete operation.
// Provide the index of the child node with the number of items that fell
// below minItems.
func (tr *Map[K, V]) nodeRebalance(n *mapNode[K, V], i int) {
	if i == len(n.Items) {
		i--
	}

	// ensure copy-on-write
	left := tr.isoLoad(&(*n.Children)[i], true)
	right := tr.isoLoad(&(*n.Children)[i+1], true)

	if len(left.Items)+len(right.Items) < tr.Maxx {
		// Merges the left and right children nodes together as a single node
		// that includes (left,item,right), and places the contents into the
		// existing left node. Delete the right node altogether and move the
		// following items and child nodes to the left by one slot.

		// merge (left,item,right)
		left.Items = append(left.Items, n.Items[i])
		left.Items = append(left.Items, right.Items...)
		if !left.leaf() {
			*left.Children = append(*left.Children, *right.Children...)
		}
		left.Count += right.Count + 1

		// move the items over one slot
		copy(n.Items[i:], n.Items[i+1:])
		n.Items[len(n.Items)-1] = tr.Empty
		n.Items = n.Items[:len(n.Items)-1]

		// move the children over one slot
		copy((*n.Children)[i+1:], (*n.Children)[i+2:])
		(*n.Children)[len(*n.Children)-1] = nil
		(*n.Children) = (*n.Children)[:len(*n.Children)-1]
	} else if len(left.Items) > len(right.Items) {
		// move left -> right over one slot

		// Move the item of the parent node at index into the right-node first
		// slot, and move the left-node last item into the previously moved
		// parent item slot.
		right.Items = append(right.Items, tr.Empty)
		copy(right.Items[1:], right.Items)
		right.Items[0] = n.Items[i]
		right.Count++
		n.Items[i] = left.Items[len(left.Items)-1]
		left.Items[len(left.Items)-1] = tr.Empty
		left.Items = left.Items[:len(left.Items)-1]
		left.Count--

		if !left.leaf() {
			// move the left-node last child into the right-node first slot
			*right.Children = append(*right.Children, nil)
			copy((*right.Children)[1:], *right.Children)
			(*right.Children)[0] = (*left.Children)[len(*left.Children)-1]
			(*left.Children)[len(*left.Children)-1] = nil
			(*left.Children) = (*left.Children)[:len(*left.Children)-1]
			left.Count -= (*right.Children)[0].Count
			right.Count += (*right.Children)[0].Count
		}
	} else {
		// move left <- right over one slot

		// Same as above but the other direction
		left.Items = append(left.Items, n.Items[i])
		left.Count++
		n.Items[i] = right.Items[0]
		copy(right.Items, right.Items[1:])
		right.Items[len(right.Items)-1] = tr.Empty
		right.Items = right.Items[:len(right.Items)-1]
		right.Count--

		if !left.leaf() {
			*left.Children = append(*left.Children, (*right.Children)[0])
			copy(*right.Children, (*right.Children)[1:])
			(*right.Children)[len(*right.Children)-1] = nil
			*right.Children = (*right.Children)[:len(*right.Children)-1]
			left.Count += (*left.Children)[len(*left.Children)-1].Count
			right.Count -= (*left.Children)[len(*left.Children)-1].Count
		}
	}
}

// Ascend the tree within the range [pivot, last]
// Pass nil for pivot to scan all item in ascending order
// Return false to stop iterating
func (tr *Map[K, V]) Ascend(pivot K, iter func(key K, value V) bool) {
	tr.ascend(pivot, iter, false)
}

func (tr *Map[K, V]) AscendMut(pivot K, iter func(key K, value V) bool) {
	tr.ascend(pivot, iter, true)
}

func (tr *Map[K, V]) ascend(pivot K, iter func(key K, value V) bool, mut bool) {
	if tr.Root == nil {
		return
	}
	tr.nodeAscend(&tr.Root, pivot, iter, mut)
}

// The return value of this function determines whether we should keep iterating
// upon this functions return.
func (tr *Map[K, V]) nodeAscend(cn **mapNode[K, V], pivot K,
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeAscend(&(*n.Children)[i], pivot, iter, mut) {
				return false
			}
		}
	}
	// We are either in the case that
	// - node is found, we should iterate through it starting at `i`,
	//   the index it was located at.
	// - node is not found, and TODO: fill in.
	for ; i < len(n.Items); i++ {
		if !iter(n.Items[i].Key, n.Items[i].Value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeScan(&(*n.Children)[i+1], iter, mut) {
				return false
			}
		}
	}
	return true
}

func (tr *Map[K, V]) Reverse(iter func(key K, value V) bool) {
	tr.reverse(iter, false)
}

func (tr *Map[K, V]) ReverseMut(iter func(key K, value V) bool) {
	tr.reverse(iter, true)
}

func (tr *Map[K, V]) reverse(iter func(key K, value V) bool, mut bool) {
	if tr.Root == nil {
		return
	}
	tr.nodeReverse(&tr.Root, iter, mut)
}

func (tr *Map[K, V]) nodeReverse(cn **mapNode[K, V],
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := len(n.Items) - 1; i >= 0; i-- {
			if !iter(n.Items[i].Key, n.Items[i].Value) {
				return false
			}
		}
		return true
	}
	if !tr.nodeReverse(&(*n.Children)[len(*n.Children)-1], iter, mut) {
		return false
	}
	for i := len(n.Items) - 1; i >= 0; i-- {
		if !iter(n.Items[i].Key, n.Items[i].Value) {
			return false
		}
		if !tr.nodeReverse(&(*n.Children)[i], iter, mut) {
			return false
		}
	}
	return true
}

// Descend the tree within the range [pivot, first]
// Pass nil for pivot to scan all item in descending order
// Return false to stop iterating
func (tr *Map[K, V]) Descend(pivot K, iter func(key K, value V) bool) {
	tr.descend(pivot, iter, false)
}

func (tr *Map[K, V]) DescendMut(pivot K, iter func(key K, value V) bool) {
	tr.descend(pivot, iter, true)
}

func (tr *Map[K, V]) descend(
	pivot K,
	iter func(key K, value V) bool,
	mut bool,
) {
	if tr.Root == nil {
		return
	}
	tr.nodeDescend(&tr.Root, pivot, iter, mut)
}

func (tr *Map[K, V]) nodeDescend(cn **mapNode[K, V], pivot K,
	iter func(key K, value V) bool, mut bool,
) bool {
	n := tr.isoLoad(cn, mut)
	i, found := tr.search(n, pivot)
	if !found {
		if !n.leaf() {
			if !tr.nodeDescend(&(*n.Children)[i], pivot, iter, mut) {
				return false
			}
		}
		i--
	}
	for ; i >= 0; i-- {
		if !iter(n.Items[i].Key, n.Items[i].Value) {
			return false
		}
		if !n.leaf() {
			if !tr.nodeReverse(&(*n.Children)[i], iter, mut) {
				return false
			}
		}
	}
	return true
}

// Load is for bulk loading pre-sorted items
func (tr *Map[K, V]) Load(key K, value V) (V, bool) {
	item := mapPair[K, V]{Key: key, Value: value}
	if tr.Root == nil {
		return tr.Set(item.Key, item.Value)
	}
	n := tr.isoLoad(&tr.Root, true)
	for {
		n.Count++ // optimistically update counts
		if n.leaf() {
			if len(n.Items) < tr.Maxx {
				if n.Items[len(n.Items)-1].Key < item.Key {
					n.Items = append(n.Items, item)
					tr.Count++
					return tr.Empty.Value, false
				}
			}
			break
		}
		n = tr.isoLoad(&(*n.Children)[len(*n.Children)-1], true)
	}
	// revert the counts
	n = tr.Root
	for {
		n.Count--
		if n.leaf() {
			break
		}
		n = (*n.Children)[len(*n.Children)-1]
	}
	return tr.Set(item.Key, item.Value)
}

// Min returns the minimum item in tree.
// Returns nil if the treex has no items.
func (tr *Map[K, V]) Min() (K, V, bool) {
	return tr.minMut(false)
}

func (tr *Map[K, V]) MinMut() (K, V, bool) {
	return tr.minMut(true)
}

func (tr *Map[K, V]) minMut(mut bool) (key K, value V, ok bool) {
	if tr.Root == nil {
		return key, value, false
	}
	n := tr.isoLoad(&tr.Root, mut)
	for {
		if n.leaf() {
			item := n.Items[0]
			return item.Key, item.Value, true
		}
		n = tr.isoLoad(&(*n.Children)[0], mut)
	}
}

// Max returns the maximum item in tree.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) Max() (K, V, bool) {
	return tr.maxMut(false)
}

func (tr *Map[K, V]) MaxMut() (K, V, bool) {
	return tr.maxMut(true)
}

func (tr *Map[K, V]) maxMut(mut bool) (K, V, bool) {
	if tr.Root == nil {
		return tr.Empty.Key, tr.Empty.Value, false
	}
	n := tr.isoLoad(&tr.Root, mut)
	for {
		if n.leaf() {
			item := n.Items[len(n.Items)-1]
			return item.Key, item.Value, true
		}
		n = tr.isoLoad(&(*n.Children)[len(*n.Children)-1], mut)
	}
}

// PopMin removes the minimum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) PopMin() (K, V, bool) {
	if tr.Root == nil {
		return tr.Empty.Key, tr.Empty.Value, false
	}
	n := tr.isoLoad(&tr.Root, true)
	var item mapPair[K, V]
	for {
		n.Count-- // optimistically update counts
		if n.leaf() {
			item = n.Items[0]
			if len(n.Items) == tr.Minn {
				break
			}
			copy(n.Items[:], n.Items[1:])
			n.Items[len(n.Items)-1] = tr.Empty
			n.Items = n.Items[:len(n.Items)-1]
			tr.Count--
			if tr.Count == 0 {
				tr.Root = nil
			}
			return item.Key, item.Value, true
		}
		n = tr.isoLoad(&(*n.Children)[0], true)
	}
	// revert the counts
	n = tr.Root
	for {
		n.Count++
		if n.leaf() {
			break
		}
		n = (*n.Children)[0]
	}
	value, deleted := tr.Delete(item.Key)
	if deleted {
		return item.Key, value, true
	}
	return tr.Empty.Key, tr.Empty.Value, false
}

// PopMax removes the maximum item in tree and returns it.
// Returns nil if the tree has no items.
func (tr *Map[K, V]) PopMax() (K, V, bool) {
	if tr.Root == nil {
		return tr.Empty.Key, tr.Empty.Value, false
	}
	n := tr.isoLoad(&tr.Root, true)
	var item mapPair[K, V]
	for {
		n.Count-- // optimistically update counts
		if n.leaf() {
			item = n.Items[len(n.Items)-1]
			if len(n.Items) == tr.Minn {
				break
			}
			n.Items[len(n.Items)-1] = tr.Empty
			n.Items = n.Items[:len(n.Items)-1]
			tr.Count--
			if tr.Count == 0 {
				tr.Root = nil
			}
			return item.Key, item.Value, true
		}
		n = tr.isoLoad(&(*n.Children)[len(*n.Children)-1], true)
	}
	// revert the counts
	n = tr.Root
	for {
		n.Count++
		if n.leaf() {
			break
		}
		n = (*n.Children)[len(*n.Children)-1]
	}
	value, deleted := tr.Delete(item.Key)
	if deleted {
		return item.Key, value, true
	}
	return tr.Empty.Key, tr.Empty.Value, false
}

// GetAt returns the value at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *Map[K, V]) GetAt(index int) (K, V, bool) {
	return tr.getAt(index, false)
}

func (tr *Map[K, V]) GetAtMut(index int) (K, V, bool) {
	return tr.getAt(index, true)
}

func (tr *Map[K, V]) getAt(index int, mut bool) (K, V, bool) {
	if tr.Root == nil || index < 0 || index >= tr.Count {
		return tr.Empty.Key, tr.Empty.Value, false
	}
	n := tr.isoLoad(&tr.Root, mut)
	for {
		if n.leaf() {
			return n.Items[index].Key, n.Items[index].Value, true
		}
		i := 0
		for ; i < len(n.Items); i++ {
			if index < (*n.Children)[i].Count {
				break
			} else if index == (*n.Children)[i].Count {
				return n.Items[i].Key, n.Items[i].Value, true
			}
			index -= (*n.Children)[i].Count + 1
		}
		n = tr.isoLoad(&(*n.Children)[i], mut)
	}
}

// DeleteAt deletes the item at index.
// Return nil if the tree is empty or the index is out of bounds.
func (tr *Map[K, V]) DeleteAt(index int) (K, V, bool) {
	if tr.Root == nil || index < 0 || index >= tr.Count {
		return tr.Empty.Key, tr.Empty.Value, false
	}
	var pathbuf [8]uint8 // track the path
	path := pathbuf[:0]
	var item mapPair[K, V]
	n := tr.isoLoad(&tr.Root, true)
outer:
	for {
		n.Count-- // optimistically update counts
		if n.leaf() {
			// the index is the item position
			item = n.Items[index]
			if len(n.Items) == tr.Minn {
				path = append(path, uint8(index))
				break outer
			}
			copy(n.Items[index:], n.Items[index+1:])
			n.Items[len(n.Items)-1] = tr.Empty
			n.Items = n.Items[:len(n.Items)-1]
			tr.Count--
			if tr.Count == 0 {
				tr.Root = nil
			}
			return item.Key, item.Value, true
		}
		i := 0
		for ; i < len(n.Items); i++ {
			if index < (*n.Children)[i].Count {
				break
			} else if index == (*n.Children)[i].Count {
				item = n.Items[i]
				path = append(path, uint8(i))
				break outer
			}
			index -= (*n.Children)[i].Count + 1
		}
		path = append(path, uint8(i))
		n = tr.isoLoad(&(*n.Children)[i], true)
	}
	// revert the counts
	n = tr.Root
	for i := 0; i < len(path); i++ {
		n.Count++
		if !n.leaf() {
			n = (*n.Children)[uint8(path[i])]
		}
	}
	value, deleted := tr.Delete(item.Key)
	if deleted {
		return item.Key, value, true
	}
	return tr.Empty.Key, tr.Empty.Value, false
}

// Height returns the height of the tree.
// Returns zero if tree has no items.
func (tr *Map[K, V]) Height() int {
	var height int
	if tr.Root != nil {
		n := tr.Root
		for {
			height++
			if n.leaf() {
				break
			}
			n = (*n.Children)[0]
		}
	}
	return height
}

// MapIter represents an iterator for btree.Map
type MapIter[K ordered, V any] struct {
	tr      *Map[K, V]
	mut     bool
	seeked  bool
	atstart bool
	atend   bool
	stack   []mapIterStackItem[K, V]
	item    mapPair[K, V]
}

type mapIterStackItem[K ordered, V any] struct {
	n *mapNode[K, V]
	i int
}

// Iter returns a read-only iterator.
func (tr *Map[K, V]) Iter() MapIter[K, V] {
	return tr.iter(false)
}

func (tr *Map[K, V]) IterMut() MapIter[K, V] {
	return tr.iter(true)
}

func (tr *Map[K, V]) iter(mut bool) MapIter[K, V] {
	var iter MapIter[K, V]
	iter.tr = tr
	iter.mut = mut
	return iter
}

// Seek to item greater-or-equal-to key.
// Returns false if there was no item found.
func (iter *MapIter[K, V]) Seek(key K) bool {
	if iter.tr == nil {
		return false
	}
	iter.seeked = true
	iter.stack = iter.stack[:0]
	if iter.tr.Root == nil {
		return false
	}
	n := iter.tr.isoLoad(&iter.tr.Root, iter.mut)
	for {
		i, found := iter.tr.search(n, key)
		iter.stack = append(iter.stack, mapIterStackItem[K, V]{n, i})
		if found {
			iter.item = n.Items[i]
			return true
		}
		if n.leaf() {
			iter.stack[len(iter.stack)-1].i--
			return iter.Next()
		}
		n = iter.tr.isoLoad(&(*n.Children)[i], iter.mut)
	}
}

// First moves iterator to first item in tree.
// Returns false if the tree is empty.
func (iter *MapIter[K, V]) First() bool {
	if iter.tr == nil {
		return false
	}
	iter.atend = false
	iter.atstart = false
	iter.seeked = true
	iter.stack = iter.stack[:0]
	if iter.tr.Root == nil {
		return false
	}
	n := iter.tr.isoLoad(&iter.tr.Root, iter.mut)
	for {
		iter.stack = append(iter.stack, mapIterStackItem[K, V]{n, 0})
		if n.leaf() {
			break
		}
		n = iter.tr.isoLoad(&(*n.Children)[0], iter.mut)
	}
	s := &iter.stack[len(iter.stack)-1]
	iter.item = s.n.Items[s.i]
	return true
}

// Last moves iterator to last item in tree.
// Returns false if the tree is empty.
func (iter *MapIter[K, V]) Last() bool {
	if iter.tr == nil {
		return false
	}
	iter.seeked = true
	iter.stack = iter.stack[:0]
	if iter.tr.Root == nil {
		return false
	}
	n := iter.tr.isoLoad(&iter.tr.Root, iter.mut)
	for {
		iter.stack = append(iter.stack, mapIterStackItem[K, V]{n, len(n.Items)})
		if n.leaf() {
			iter.stack[len(iter.stack)-1].i--
			break
		}
		n = iter.tr.isoLoad(&(*n.Children)[len(n.Items)], iter.mut)
	}
	s := &iter.stack[len(iter.stack)-1]
	iter.item = s.n.Items[s.i]
	return true
}

// Next moves iterator to the next item in iterator.
// Returns false if the tree is empty or the iterator is at the end of
// the tree.
func (iter *MapIter[K, V]) Next() bool {
	if iter.tr == nil {
		return false
	}
	if !iter.seeked {
		return iter.First()
	}
	if len(iter.stack) == 0 {
		if iter.atstart {
			return iter.First() && iter.Next()
		}
		return false
	}
	s := &iter.stack[len(iter.stack)-1]
	s.i++
	if s.n.leaf() {
		if s.i == len(s.n.Items) {
			for {
				iter.stack = iter.stack[:len(iter.stack)-1]
				if len(iter.stack) == 0 {
					iter.atend = true
					return false
				}
				s = &iter.stack[len(iter.stack)-1]
				if s.i < len(s.n.Items) {
					break
				}
			}
		}
	} else {
		n := iter.tr.isoLoad(&(*s.n.Children)[s.i], iter.mut)
		for {
			iter.stack = append(iter.stack, mapIterStackItem[K, V]{n, 0})
			if n.leaf() {
				break
			}
			n = iter.tr.isoLoad(&(*n.Children)[0], iter.mut)
		}
	}
	s = &iter.stack[len(iter.stack)-1]
	iter.item = s.n.Items[s.i]
	return true
}

// Prev moves iterator to the previous item in iterator.
// Returns false if the tree is empty or the iterator is at the beginning of
// the tree.
func (iter *MapIter[K, V]) Prev() bool {
	if iter.tr == nil {
		return false
	}
	if !iter.seeked {
		return false
	}
	if len(iter.stack) == 0 {
		if iter.atend {
			return iter.Last() && iter.Prev()
		}
		return false
	}
	s := &iter.stack[len(iter.stack)-1]
	if s.n.leaf() {
		s.i--
		if s.i == -1 {
			for {
				iter.stack = iter.stack[:len(iter.stack)-1]
				if len(iter.stack) == 0 {
					iter.atstart = true
					return false
				}
				s = &iter.stack[len(iter.stack)-1]
				s.i--
				if s.i > -1 {
					break
				}
			}
		}
	} else {
		n := iter.tr.isoLoad(&(*s.n.Children)[s.i], iter.mut)
		for {
			iter.stack = append(iter.stack,
				mapIterStackItem[K, V]{n, len(n.Items)})
			if n.leaf() {
				iter.stack[len(iter.stack)-1].i--
				break
			}
			n = iter.tr.isoLoad(&(*n.Children)[len(n.Items)], iter.mut)
		}
	}
	s = &iter.stack[len(iter.stack)-1]
	iter.item = s.n.Items[s.i]
	return true
}

// Key returns the current iterator item key.
func (iter *MapIter[K, V]) Key() K {
	return iter.item.Key
}

// Value returns the current iterator item value.
func (iter *MapIter[K, V]) Value() V {
	return iter.item.Value
}

// Values returns all the values in order.
func (tr *Map[K, V]) Values() []V {
	return tr.values(false)
}

func (tr *Map[K, V]) ValuesMut() []V {
	return tr.values(true)
}

func (tr *Map[K, V]) values(mut bool) []V {
	values := make([]V, 0, tr.Len())
	if tr.Root != nil {
		values = tr.nodeValues(&tr.Root, values, mut)
	}
	return values
}

func (tr *Map[K, V]) nodeValues(cn **mapNode[K, V], values []V, mut bool) []V {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.Items); i++ {
			values = append(values, n.Items[i].Value)
		}
		return values
	}
	for i := 0; i < len(n.Items); i++ {
		values = tr.nodeValues(&(*n.Children)[i], values, mut)
		values = append(values, n.Items[i].Value)
	}
	return tr.nodeValues(&(*n.Children)[len(*n.Children)-1], values, mut)
}

// Keys returns all the keys in order.
func (tr *Map[K, V]) Keys() []K {
	keys := make([]K, 0, tr.Len())
	if tr.Root != nil {
		keys = tr.Root.keys(keys)
	}
	return keys
}

func (n *mapNode[K, V]) keys(keys []K) []K {
	if n.leaf() {
		for i := 0; i < len(n.Items); i++ {
			keys = append(keys, n.Items[i].Key)
		}
		return keys
	}
	for i := 0; i < len(n.Items); i++ {
		keys = (*n.Children)[i].keys(keys)
		keys = append(keys, n.Items[i].Key)
	}
	return (*n.Children)[len(*n.Children)-1].keys(keys)
}

// KeyValues returns all the keys and values in order.
func (tr *Map[K, V]) KeyValues() ([]K, []V) {
	return tr.keyValues(false)
}

func (tr *Map[K, V]) KeyValuesMut() ([]K, []V) {
	return tr.keyValues(true)
}

func (tr *Map[K, V]) keyValues(mut bool) ([]K, []V) {
	keys := make([]K, 0, tr.Len())
	values := make([]V, 0, tr.Len())
	if tr.Root != nil {
		keys, values = tr.nodeKeyValues(&tr.Root, keys, values, mut)
	}
	return keys, values
}

func (tr *Map[K, V]) nodeKeyValues(cn **mapNode[K, V], keys []K, values []V,
	mut bool,
) ([]K, []V) {
	n := tr.isoLoad(cn, mut)
	if n.leaf() {
		for i := 0; i < len(n.Items); i++ {
			keys = append(keys, n.Items[i].Key)
			values = append(values, n.Items[i].Value)
		}
		return keys, values
	}
	for i := 0; i < len(n.Items); i++ {
		keys, values = tr.nodeKeyValues(&(*n.Children)[i], keys, values, mut)
		keys = append(keys, n.Items[i].Key)
		values = append(values, n.Items[i].Value)
	}
	return tr.nodeKeyValues(&(*n.Children)[len(*n.Children)-1], keys, values,
		mut)
}

// Clear will delete all items.
func (tr *Map[K, V]) Clear() {
	tr.Count = 0
	tr.Root = nil
}
