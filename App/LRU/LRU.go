package main

import (
	"container/list"
	"fmt"
)

type Cache interface {
	Get(key string) string
	Set(key, value string)
}

type cacheMapElement struct {
	el    *list.Element
	value string
}

type LRUCache struct {
	m   map[string]*cacheMapElement
	cap int
	l   list.List
}

func NewLRU(cap int) LRUCache {
	return LRUCache{
		m:   map[string]*cacheMapElement{},
		cap: cap,
		l:   list.List{},
	}
}

func (lru *LRUCache) Get(key string) string {
	cMapEl, exists := lru.m[key]
	if !exists {
		return ""
	} else {
		lru.l.MoveToFront(cMapEl.el)
		return cMapEl.value
	}
}

func (lru *LRUCache) Set(key, value string) {
	cMapEl, exists := lru.m[key]
	if !exists {
		newEl := lru.l.PushFront(key)
		lru.m[key] = &cacheMapElement{
			el:    newEl,
			value: value,
		}

		if lru.l.Len() > lru.cap {
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
	cache.Set("asd", "vule")
	cache.Set("aaa", "jole")
	cache.Set("bbb", "dule")
	cache.Get("asd")
	fmt.Println(cache.l.Front())
	cache.Set("sss", "bule")
}
