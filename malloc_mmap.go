//go:build !appengine && !windows
// +build !appengine,!windows

package fastcache

import (
	"fmt"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

const chunksPerAlloc = 1024

var (
	freeChunks     []*[chunkSize]byte
	freeChunksLock sync.Mutex
)

func getChunk() []byte {
	freeChunksLock.Lock()
	// 检查是否有可用的内存块，如果没有，则开辟
	if len(freeChunks) == 0 {
		// Allocate offheap memory, so GOGC won't take into account cache size.
		// This should reduce free memory waste.
		//使用 unix.Mmap 分配一块较大的匿名内存区域 (chunkSize*chunksPerAlloc 字节)，这块内存不会被 Go 的垃圾回收器（GOGC）计入，从而减少内存浪费。
		data, err := unix.Mmap(-1, 0, chunkSize*chunksPerAlloc, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_ANON|unix.MAP_PRIVATE)
		if err != nil {
			panic(fmt.Errorf("cannot allocate %d bytes via mmap: %s", chunkSize*chunksPerAlloc, err))
		}
		//将这块大内存分割成多个 chunkSize 大小的小块，每个小块被添加到 freeChunks 切片中。data 切片被逐步分割并转换成 *chunkSize 类型的指针
		for len(data) > 0 {
			p := (*[chunkSize]byte)(unsafe.Pointer(&data[0]))
			freeChunks = append(freeChunks, p)
			data = data[chunkSize:]
		}
	}
	//从 freeChunks 切片中取出最后一个块，将其从切片中移除，并将其内容清空以防止泄露
	n := len(freeChunks) - 1
	p := freeChunks[n]
	freeChunks[n] = nil
	freeChunks = freeChunks[:n]
	freeChunksLock.Unlock()
	return p[:]
}

func putChunk(chunk []byte) {
	if chunk == nil {
		return
	}
	chunk = chunk[:chunkSize]
	p := (*[chunkSize]byte)(unsafe.Pointer(&chunk[0]))

	freeChunksLock.Lock()
	freeChunks = append(freeChunks, p)
	freeChunksLock.Unlock()
}
