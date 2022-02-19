package tokenBucket

import (
	"time"
)

type TokenBucket struct {
	maxTries       uint32
	remainingTries uint32
	resetTimer     uint32
	lastTimestamp  time.Time
}

func NewTokenBucket(maxTries, resetTimer uint32) TokenBucket {
	tb := TokenBucket{
		maxTries:       maxTries,
		remainingTries: maxTries,
		resetTimer:     resetTimer,
		lastTimestamp:  time.Time{},
	}

	return tb
}

func (tb *TokenBucket) CheckInputTimer() bool {
	if time.Now().Sub(tb.lastTimestamp).Seconds() >= float64(tb.resetTimer) {
		tb.remainingTries = tb.maxTries
		tb.lastTimestamp = time.Now()
	}
	if tb.remainingTries > 0 {
		tb.remainingTries--
		return true
	} else {
		return false
	}
}
