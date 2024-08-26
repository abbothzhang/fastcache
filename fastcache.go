// Package fastcache implements fast in-memory cache.
//
// The package has been extracted from https://victoriametrics.com/
package fastcache

import (
	"fmt"
	"sync"
	"sync/atomic"

	xxhash "github.com/cespare/xxhash/v2"
)

// 512个分片
const bucketsCount = 512

// 块大小
const chunkSize = 64 * 1024

// 40个比特位
const bucketSizeBits = 40

const genSizeBits = 64 - bucketSizeBits

const maxGen = 1<<genSizeBits - 1

// 每个桶最大为1TB
const maxBucketSize uint64 = 1 << bucketSizeBits

// Stats represents cache stats.
//
// Use Cache.UpdateStats for obtaining fresh stats from the cache.
type Stats struct {
	// GetCalls is the number of Get calls.
	GetCalls uint64

	// SetCalls is the number of Set calls.
	SetCalls uint64

	// Misses is the number of cache misses.
	Misses uint64

	// Collisions is the number of cache collisions.
	//
	// Usually the number of collisions must be close to zero.
	// High number of collisions suggest something wrong with cache.
	Collisions uint64

	// Corruptions is the number of detected corruptions of the cache.
	//
	// Corruptions may occur when corrupted cache is loaded from file.
	Corruptions uint64

	// EntriesCount is the current number of entries in the cache.
	EntriesCount uint64

	// BytesSize is the current size of the cache in bytes.
	BytesSize uint64

	// MaxBytesSize is the maximum allowed size of the cache in bytes (aka capacity).
	MaxBytesSize uint64

	// BigStats contains stats for GetBig/SetBig methods.
	BigStats
}

// Reset resets s, so it may be re-used again in Cache.UpdateStats.
func (s *Stats) Reset() {
	*s = Stats{}
}

// BigStats contains stats for GetBig/SetBig methods.
type BigStats struct {
	// GetBigCalls is the number of GetBig calls.
	GetBigCalls uint64

	// SetBigCalls is the number of SetBig calls.
	SetBigCalls uint64

	// TooBigKeyErrors is the number of calls to SetBig with too big key.
	TooBigKeyErrors uint64

	// InvalidMetavalueErrors is the number of calls to GetBig resulting
	// to invalid metavalue.
	InvalidMetavalueErrors uint64

	// InvalidValueLenErrors is the number of calls to GetBig resulting
	// to a chunk with invalid length.
	InvalidValueLenErrors uint64

	// InvalidValueHashErrors is the number of calls to GetBig resulting
	// to a chunk with invalid hash value.
	InvalidValueHashErrors uint64
}

func (bs *BigStats) reset() {
	atomic.StoreUint64(&bs.GetBigCalls, 0)
	atomic.StoreUint64(&bs.SetBigCalls, 0)
	atomic.StoreUint64(&bs.TooBigKeyErrors, 0)
	atomic.StoreUint64(&bs.InvalidMetavalueErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueLenErrors, 0)
	atomic.StoreUint64(&bs.InvalidValueHashErrors, 0)
}

// Cache is a fast thread-safe inmemory cache optimized for big number
// of entries.
//
// It has much lower impact on GC comparing to a simple `map[string][]byte`.
//
// Use New or LoadFromFile* for creating new cache instance.
// Concurrent goroutines may call any Cache methods on the same cache instance.
//
// Call Reset when the cache is no longer needed. This reclaims the allocated
// memory.
type Cache struct {
	buckets [bucketsCount]bucket

	bigStats BigStats
}

// New returns new cache with the given maxBytes capacity in bytes.
//
// maxBytes must be smaller than the available RAM size for the app,
// since the cache holds data in memory.
//
// If maxBytes is less than 32MB, then the minimum cache capacity is 32MB.
func New(maxBytes int) *Cache {
	if maxBytes <= 0 {
		panic(fmt.Errorf("maxBytes must be greater than 0; got %d", maxBytes))
	}
	var c Cache
	// 计算每个桶能够存储的最大字节数
	maxBucketBytes := uint64((maxBytes + bucketsCount - 1) / bucketsCount)
	// 初始化每个桶的容量
	for i := range c.buckets[:] {
		c.buckets[i].Init(maxBucketBytes)
	}
	return &c
}

// Set stores (k, v) in the cache.
//
// Get must be used for reading the stored entry.
//
// The stored entry may be evicted at any time either due to cache
// overflow or due to unlikely hash collision.
// Pass higher maxBytes value to New if the added items disappear
// frequently.
//
// (k, v) entries with summary size exceeding 64KB aren't stored in the cache.
// SetBig can be used for storing entries exceeding 64KB.
//
// k and v contents may be modified after returning from Set.
func (c *Cache) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Set(k, v, h)
}

// Get appends value by the key k to dst and returns the result.
//
// Get allocates new byte slice for the returned value if dst is nil.
//
// Get returns only values stored in c via Set.
//
// k contents may be modified after returning from Get.
// 如果 dst 为 nil，则 Get 方法会为返回的值分配一个新的字节切片
func (c *Cache) Get(dst, key []byte) []byte {
	hash := xxhash.Sum64(key)
	idx := hash % bucketsCount
	dst, _ = c.buckets[idx].Get(dst, key, hash, true)
	return dst
}

// HasGet works identically to Get, but also returns whether the given key
// exists in the cache. This method makes it possible to differentiate between a
// stored nil/empty value versus and non-existing value.
func (c *Cache) HasGet(dst, k []byte) ([]byte, bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	return c.buckets[idx].Get(dst, k, h, true)
}

// Has returns true if entry for the given key k exists in the cache.
func (c *Cache) Has(k []byte) bool {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	_, ok := c.buckets[idx].Get(nil, k, h, false)
	return ok
}

// Del deletes value for the given k from the cache.
//
// k contents may be modified after returning from Del.
func (c *Cache) Del(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsCount
	c.buckets[idx].Del(h)
}

// Reset removes all the items from the cache.
func (c *Cache) Reset() {
	for i := range c.buckets[:] {
		c.buckets[i].Reset()
	}
	c.bigStats.reset()
}

// UpdateStats adds cache stats to s.
//
// Call s.Reset before calling UpdateStats if s is re-used.
func (c *Cache) UpdateStats(s *Stats) {
	for i := range c.buckets[:] {
		c.buckets[i].UpdateStats(s)
	}
	s.GetBigCalls += atomic.LoadUint64(&c.bigStats.GetBigCalls)
	s.SetBigCalls += atomic.LoadUint64(&c.bigStats.SetBigCalls)
	s.TooBigKeyErrors += atomic.LoadUint64(&c.bigStats.TooBigKeyErrors)
	s.InvalidMetavalueErrors += atomic.LoadUint64(&c.bigStats.InvalidMetavalueErrors)
	s.InvalidValueLenErrors += atomic.LoadUint64(&c.bigStats.InvalidValueLenErrors)
	s.InvalidValueHashErrors += atomic.LoadUint64(&c.bigStats.InvalidValueHashErrors)
}

type bucket struct {
	mu sync.RWMutex

	// chunks is a ring buffer with encoded (k, v) pairs.
	// It consists of 64KB chunks.
	chunks [][]byte

	// m maps hash(k) to idx of (k, v) pair in chunks.
	m map[uint64]uint64

	// idx points to chunks for writing the next (k, v) pair.
	idx uint64

	// gen is the generation of chunks.
	gen uint64

	getCalls    uint64
	setCalls    uint64
	misses      uint64
	collisions  uint64
	corruptions uint64
}

func (b *bucket) Init(maxBytes uint64) {
	if maxBytes == 0 {
		panic(fmt.Errorf("maxBytes cannot be zero"))
	}
	// 每个桶最大为1TB
	if maxBytes >= maxBucketSize {
		panic(fmt.Errorf("too big maxBytes=%d; should be smaller than %d", maxBytes, maxBucketSize))
	}
	maxChunks := (maxBytes + chunkSize - 1) / chunkSize
	b.chunks = make([][]byte, maxChunks)
	b.m = make(map[uint64]uint64)
	b.Reset()
}

func (b *bucket) Reset() {
	b.mu.Lock()
	chunks := b.chunks
	for i := range chunks {
		putChunk(chunks[i])
		chunks[i] = nil
	}
	b.m = make(map[uint64]uint64)
	b.idx = 0
	b.gen = 1
	atomic.StoreUint64(&b.getCalls, 0)
	atomic.StoreUint64(&b.setCalls, 0)
	atomic.StoreUint64(&b.misses, 0)
	atomic.StoreUint64(&b.collisions, 0)
	atomic.StoreUint64(&b.corruptions, 0)
	b.mu.Unlock()
}

func (b *bucket) cleanLocked() {
	bGen := b.gen & ((1 << genSizeBits) - 1)
	bIdx := b.idx
	bm := b.m
	newItems := 0
	for _, v := range bm {
		gen := v >> bucketSizeBits
		idx := v & ((1 << bucketSizeBits) - 1)
		if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
			newItems++
		}
	}
	if newItems < len(bm) {
		// Re-create b.m with valid items, which weren't expired yet instead of deleting expired items from b.m.
		// This should reduce memory fragmentation and the number Go objects behind b.m.
		// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/5379
		bmNew := make(map[uint64]uint64, newItems)
		for k, v := range bm {
			gen := v >> bucketSizeBits
			idx := v & ((1 << bucketSizeBits) - 1)
			if (gen+1 == bGen || gen == maxGen && bGen == 1) && idx >= bIdx || gen == bGen && idx < bIdx {
				bmNew[k] = v
			}
		}
		b.m = bmNew
	}
}

func (b *bucket) UpdateStats(s *Stats) {
	s.GetCalls += atomic.LoadUint64(&b.getCalls)
	s.SetCalls += atomic.LoadUint64(&b.setCalls)
	s.Misses += atomic.LoadUint64(&b.misses)
	s.Collisions += atomic.LoadUint64(&b.collisions)
	s.Corruptions += atomic.LoadUint64(&b.corruptions)

	b.mu.RLock()
	s.EntriesCount += uint64(len(b.m))
	bytesSize := uint64(0)
	for _, chunk := range b.chunks {
		bytesSize += uint64(cap(chunk))
	}
	s.BytesSize += bytesSize
	s.MaxBytesSize += uint64(len(b.chunks)) * chunkSize
	b.mu.RUnlock()
}

func (b *bucket) Set(k, v []byte, h uint64) {
	// 原子地增加存储调用次数计数器
	atomic.AddUint64(&b.setCalls, 1)
	// 如果键 k 或值 v 的长度大于等于 65536（1<<16），方法会返回，因为这些长度超出了用两个字节编码的范围
	if len(k) >= (1<<16) || len(v) >= (1<<16) {
		// Too big key or value - its length cannot be encoded
		// with 2 bytes (see below). Skip the entry.
		return
	}
	// 长度信息是用 16 位整数表示的，因此需要两个字节来存储它。高 8 位和低 8 位分别存储在两个字节中
	//vLenBuf：用 4 字节存储键和值的长度（各用 2 字节编码），分别存储键的高 8 位和低 8 位长度，以及值的高 8 位和低 8 位长度。
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	// byte(len(k)) 只保留了 len(k) 的低 8 位
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
	//kvLen：计算键值对的总长度，包括 kvLenBuf、键 k 和值 v 的长度
	kvLen := uint64(len(kvLenBuf) + len(k) + len(v))
	// 如果 kvLen 大于或等于 chunkSize（块大小），方法返回，因为键值对太大，不能存储在一个块中
	if kvLen >= chunkSize {
		// Do not store too big keys and values, since they do not
		// fit a chunk.
		return
	}

	chunks := b.chunks
	needClean := false
	b.mu.Lock()
	idx := b.idx
	//计算新的写入位置：idxNew 是在当前索引 idx 的基础上加上 kvLen（键值对的总长度），计算出插入操作后的新位置。
	idxNew := idx + kvLen
	//计算 chunkIdx（当前块索引）和 chunkIdxNew（新块索引）
	chunkIdx := idx / chunkSize
	chunkIdxNew := idxNew / chunkSize
	//如果新块索引超出了现有块的范围，需要新创建块
	//如果超出块数组长度，重置索引和长度，增加生成代数 b.gen，并可能清理旧块。
	//否则，调整当前块的起始索引
	if chunkIdxNew > chunkIdx {
		// 如果新的块索引 chunkIdxNew 超过了当前已分配的块的数量（即 chunks 切片的长度），说明需要重新初始化块
		if chunkIdxNew >= uint64(len(chunks)) {
			//将 idx 和 chunkIdx 重置为 0，并将 idxNew 设为 kvLen，这表示从新的块开始写入数据
			idx = 0
			idxNew = kvLen
			chunkIdx = 0
			//b.gen 是用于生成新的块标识符的代数。增加生成代数，并在生成代数满足一定条件时（如位掩码操作），进行额外的增加操作。
			//这通常用于生成唯一的块版本标识符，帮助区分不同版本的块
			b.gen++
			if b.gen&((1<<genSizeBits)-1) == 0 {
				b.gen++
			}
			//设定 needClean 为 true，表示需要清理旧的块（或做其他必要的管理操作），这通常是在块已满或达到一定的容量时进行的维护操作
			needClean = true
		} else {
			//如果 chunkIdxNew 没有超过现有块的数量，则更新当前索引 idx 和新的索引 idxNew，并设置 chunkIdx 为 chunkIdxNew。
			//这表示继续在当前块内写入数据，更新索引以反映新的写入位置
			idx = chunkIdxNew * chunkSize
			idxNew = idx + kvLen
			chunkIdx = chunkIdxNew
		}
		//清空当前块 chunks[chunkIdx] 的内容。
		//虽然 chunks[chunkIdx] 被重新分配内存，
		//但这一步骤确保当前块的内容被清空，以便新的数据可以被正确地追加到块中
		// todo:2024/8/26 为什么要清理当前块数据
		chunks[chunkIdx] = chunks[chunkIdx][:0]
	}
	//获取或创建块 chunk。
	chunk := chunks[chunkIdx]
	if chunk == nil {
		chunk = getChunk()
		chunk = chunk[:0]
	}
	//将 kvLenBuf、键 k 和值 v 附加到块中。
	chunk = append(chunk, kvLenBuf[:]...)
	chunk = append(chunk, k...)
	chunk = append(chunk, v...)
	//更新 chunks[chunkIdx] 为新的块内容。
	chunks[chunkIdx] = chunk
	//更新哈希表 b.m 以映射哈希值 h 到当前的存储位置和生成代数。
	b.m[h] = idx | (b.gen << bucketSizeBits)
	//更新桶的索引 b.idx 为新的位置
	b.idx = idxNew
	if needClean {
		b.cleanLocked()
	}
	b.mu.Unlock()
}

func (b *bucket) Get(dst, key []byte, hash uint64, returnDst bool) ([]byte, bool) {
	atomic.AddUint64(&b.getCalls, 1)
	// 初始化 found 变量为 false，表示默认没有找到匹配的数据
	found := false
	chunks := b.chunks
	b.mu.RLock()       // 获取只读锁，确保线程安全地访问 b.m 和 b.gen
	value := b.m[hash] // 从 b.m 中根据 hash 查找对应的值
	// bGen 获取当前桶的生成代数，确保正确的桶版本被访问
	// 通过位掩码 (1 << genSizeBits) - 1，bGen 提取了 b.gen 的低 genSizeBits 位。这个掩码确保只保留生成代数的有效部分，忽略其他位
	bGen := b.gen & ((1 << genSizeBits) - 1)

	if value > 0 { // 如果 value 大于 0，说明存在可能的有效数据
		// 检查 v 是否有效且符合当前代数 bGen
		// 从 value 中提取生成代数 gen 和索引 idx。bucketSizeBits 表示索引部分的位数
		gen := value >> bucketSizeBits
		idx := value & ((1 << bucketSizeBits) - 1)
		// 检查提取的生成代数和索引是否有效。确保数据没有被回收或被其他操作覆盖
		if gen == bGen && idx < b.idx || gen+1 == bGen && idx >= b.idx || gen == maxGen && bGen == 1 && idx >= b.idx {
			// 计算数据块的索引
			chunkIdx := idx / chunkSize
			if chunkIdx >= uint64(len(chunks)) {
				// 如果计算出的 chunkIdx 超出了 chunks 的范围，说明数据可能在文件加载过程中被损坏。
				// 增加腐败计数器，然后跳转到 end 标签以解锁资源并返回。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			chunk := chunks[chunkIdx]
			idx %= chunkSize
			if idx+4 >= chunkSize {
				// 如果计算出的索引加上 4 超出了 chunk 的范围，说明数据可能在文件加载过程中被损坏。
				// 增加腐败计数器，然后跳转到 end 标签以解锁资源并返回。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			kvLenBuf := chunk[idx : idx+4]                             // 提取包含键值长度的 4 字节数据
			keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1]) // 解析键的长度
			valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3]) // 解析值的长度
			idx += 4
			if idx+keyLen+valLen >= chunkSize {
				// 如果计算出的索引加上 keyLen 和 valLen 超出了 chunk 的范围，说明数据可能在文件加载过程中被损坏。
				// 增加腐败计数器，然后跳转到 end 标签以解锁资源并返回。
				atomic.AddUint64(&b.corruptions, 1)
				goto end
			}
			if string(key) == string(chunk[idx:idx+keyLen]) { // 如果键匹配
				idx += keyLen
				if returnDst { // 如果 returnDst 为 true，将值追加到 dst
					dst = append(dst, chunk[idx:idx+valLen]...)
				}
				found = true
			} else {
				// 如果键不匹配，增加冲突计数器
				atomic.AddUint64(&b.collisions, 1)
			}
		}
	}
end:
	b.mu.RUnlock() // 释放只读锁
	if !found {
		// 如果没有找到匹配项，增加未命中计数器
		atomic.AddUint64(&b.misses, 1)
	}
	return dst, found // 返回结果
}

func (b *bucket) Del(h uint64) {
	b.mu.Lock()
	delete(b.m, h)
	b.mu.Unlock()
}
