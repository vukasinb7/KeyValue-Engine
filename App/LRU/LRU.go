package lru

import (
	"container/list"
)

type Cache interface {
	Get(key string) []byte
	Set(key string, value []byte)
}

type cacheMapElement struct {
	el    *list.Element
	value []byte
}

type LRUCache struct {
	m   map[string]*cacheMapElement
	cap uint32
	l   list.List
}

func NewLRU(cap uint32) LRUCache {
	return LRUCache{
		m:   map[string]*cacheMapElement{},
		cap: cap,
		l:   list.List{},
	}
}

func (lru *LRUCache) Get(key string) []byte {
	cMapEl, exists := lru.m[key]
	if !exists {
		return nil
	} else {
		lru.l.MoveToFront(cMapEl.el)
		return cMapEl.value
	}
}

func (lru *LRUCache) Set(key string, value []byte) {
	cMapEl, exists := lru.m[key]
	if !exists {
		newEl := lru.l.PushFront(key)
		lru.m[key] = &cacheMapElement{
			el:    newEl,
			value: value,
		}

		if uint32(lru.l.Len()) > lru.cap {
			backEl := lru.l.Back()
			backElementKey := backEl.Value.(string)
			lru.l.Remove(backEl)
			delete(lru.m, backElementKey)
		}
	} else {
		cMapEl.value = value
		lru.l.MoveToFront(cMapEl.el)
	}
}

func main() {
	cache := NewLRU(3)
	cache.Set("asd", []byte("vule"))
	cache.Set("aaa", []byte("jole"))
	cache.Set("bbb", []byte("dule"))
	cache.Set("sss", []byte("bule"))

}
