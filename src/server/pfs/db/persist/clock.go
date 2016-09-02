package persist

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dancannon/gorethink"
)

func (b *BlockRef) Size() uint64 {
	return b.Upper - b.Lower
}

// NewClock returns a new clock for a given branch
func NewClock(branch string) *Clock {
	return &Clock{branch, 0}
}

func ClockEq(c1 *Clock, c2 *Clock) bool {
	return c1.Branch == c2.Branch && c1.Clock == c2.Clock
}

func CloneClock(c *Clock) *Clock {
	return &Clock{
		Branch: c.Branch,
		Clock:  c.Clock,
	}
}

// "master/2"
func StringToClock(s string) (*Clock, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid clock string: %s", s)
	}
	clock, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid clock string: %v", err)
	}
	return &Clock{
		Branch: parts[0],
		Clock:  uint64(clock),
	}, nil
}

// NewChild returns the child of a FullClock
// [(master, 0), (foo, 0)] -> [(master, 0), (foo, 1)]
func NewChild(parent FullClock) FullClock {
	if len(parent) == 0 {
		return parent
	} else {
		lastClock := CloneClock(FullClockHead(parent))
		lastClock.Clock += 1
		return append(parent[:len(parent)-1], lastClock)
	}
}

// FullClockParent returns the parent of a full clock, or nil if the clock has no parent
// [(master, 2), (foo, 1)] -> [(master, 2), (foo, 0)]
// [(master, 2), (foo, 0)] -> [(master, 2)]
func FullClockParent(child FullClock) FullClock {
	if len(child) > 0 {
		lastClock := CloneClock(FullClockHead(child))
		if lastClock.Clock > 0 {
			lastClock.Clock -= 1
			return append(child[:len(child)-1], lastClock)
		} else if len(child) > 1 {
			return child[:len(child)-1]
		}
	}
	return nil
}

// FullClock is an array of clocks, e.g. [(master, 2), (foo, 3)]
type FullClock []*Clock

/*
func (fc FullClock) Size() int {
	return len(fc)
}

// ToArray converts a FullClock to an array of arrays.
// This is useful in indexing BranchClocks in RethinkDB.
func (fc FullClock) ToArray() (res []interface{}) {
	for _, clock := range fc {
		res = append(res, []interface{}{clock.Branch, clock.Clock})
	}
	return res
}*/
func FullClockHead(fc FullClock) *Clock {
	if len(fc) == 0 {
		return nil
	}
	return fc[len(fc)-1]
}

func FullClockBranch(fc FullClock) string {
	return FullClockHead(fc).Branch
}

// BranchClockToArray converts a BranchClock to an array.
// Putting this function here so it stays in sync with ToArray.
func FullClockToArray(fullClock gorethink.Term) gorethink.Term {
	return fullClock.Map(func(clock gorethink.Term) []interface{} {
		return []interface{}{clock.Field("Branch"), clock.Field("Clock")}
	})
}

func (c *Clock) ToArray() []interface{} {
	return []interface{}{c.Branch, c.Clock}
}

func ClockToArray(clock gorethink.Term) []interface{} {
	return []interface{}{clock.Field("Branch"), clock.Field("Clock")}
}

func (c *Clock) ToCommitID() string {
	return fmt.Sprintf("%s/%d", c.Branch, c.Clock)
}

func (d *Diff) CommitID() string {
	return d.Clock.ToCommitID()
}

// A ClockRangeList is an ordered list of ClockRanges
type ClockRangeList struct {
	ranges []*ClockRange
}

// A ClockRange represents a range of clocks
type ClockRange struct {
	Branch string
	Left   uint64
	Right  uint64
}

// NewClockRangeList creates a ClockRangeList that represents all clock ranges
// in between the two given FullClocks.
func NewClockRangeList(from FullClock, to FullClock) ClockRangeList {
	var crl ClockRangeList
	crl.AddFullClock(to)
	crl.SubFullClock(from)
	return crl
}

func (l *ClockRangeList) AddFullClock(fc FullClock) {
	for _, c := range fc {
		l.AddClock(c)
	}
}

// AddClock adds a range [0, c.Clock]
func (l *ClockRangeList) AddClock(c *Clock) {
	for _, r := range l.ranges {
		if r.Branch == c.Branch {
			if c.Clock > r.Right {
				r.Right = c.Clock
			}
			return
		}
	}
	l.ranges = append(l.ranges, &ClockRange{
		Branch: c.Branch,
		Left:   0,
		Right:  c.Clock,
	})
}

func (l *ClockRangeList) SubFullClock(fc FullClock) {
	for _, c := range fc {
		l.SubClock(c)
	}
}

// SubClock substracts a range [0, c.Clock]
func (l *ClockRangeList) SubClock(c *Clock) {
	// only keep non-empty ranges
	var newRanges []*ClockRange
	for _, r := range l.ranges {
		if r.Branch == c.Branch {
			r.Left = c.Clock + 1
		}
		if r.Left <= r.Right {
			newRanges = append(newRanges, r)
		}
	}
	l.ranges = newRanges
}

func (l *ClockRangeList) Ranges() []*ClockRange {
	return l.ranges
}
