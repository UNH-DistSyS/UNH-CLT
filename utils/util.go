package utils

import (
	"fmt"
	"math/rand"
	"time"
)

const MaxUint = ^uint(0)
const MinUint = 0
const MaxInt = int(MaxUint >> 1) // 1 bit for 2's compliment
const MinInt = -MaxInt - 1
const MaxUint64 = ^uint64(0)
const MinUint64 = 0
const MaxInt64 = int(MaxUint64 >> 1) // 1 bit for 2's compliment
const MinInt64 = -MaxInt64 - 1
const MaxUint32 = ^uint32(0)
const MinUint32 = 0
const MaxInt32 = int32(MaxUint32 >> 1) // 1 bit for 2's compliment
const MinInt32 = -MaxInt32 - 1

func CurrentTimeInMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func NsToMs(timeNs int64) int64 {
	return timeNs / int64(time.Millisecond)
}

// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// Uint64Max of two int
func Uint64Max(a, b uint64) uint64 {
	if a < b {
		return b
	}
	return a
}

// Max of two int
func Min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func Pow(a, b int) int {
	power := 1
	for b > 0 {
		if b&1 != 0 {
			power *= a
		}
		b >>= 1
		a *= a
	}
	return power
}

// VMax of a vector
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

// Retry function f sleep time between attempts
func Retry(f func() error, attempts int, sleep time.Duration) error {
	var err error
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return nil
		}
		if i >= attempts-1 {
			break
		}

		// exponential delay
		time.Sleep(sleep * time.Duration(i+1))
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// Schedule repeatedly call function with intervals
func Schedule(f func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			f()
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}

func AreByteStringsSame(b1 []byte, b2 []byte) bool {
	if len(b1) != len(b2) {
		return false
	}

	for i, b := range b1 {
		if b2[i] != b {
			return false
		}
	}

	return true
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandString(length int, rnd *rand.Rand) string {
	b := make([]byte, length)
	charLen := len(charset)
	for i := range b {
		rn := rnd.Intn(charLen)
		b[i] = charset[rn]
	}
	return string(b)
}
