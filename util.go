package gframe

import (
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
)

const SortMax = 2048

func Parallel(threads int, f func(id int)) (reterr error) {
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			reterr = fmt.Errorf("Error panics in parallel, %v, stack = %s", err, stack)
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f(i)
		}(i)
	}

	wg.Wait()
	return
}

func CompareStrList(a, b []string) (intr, amb, bma []string) {
	aa := make([]string, len(a))
	copy(aa, a)
	sort.Strings(aa)

	bb := make([]string, len(b))
	copy(bb, b)
	sort.Strings(bb)

	var i, j int
	for i < len(aa) && j < len(bb) {
		if aa[i] == bb[j] {
			intr = append(intr, aa[i])
			i++
			j++
		} else if aa[i] < bb[j] {
			amb = append(amb, aa[i])
			i++
		} else {
			bma = append(bma, bb[j])
			j++
		}
	}

	amb = append(amb, aa[i:]...)
	bma = append(bma, bb[j:]...)
	return
}

func sliceSelectFloat32(src []float32, ids []int32) (ret []float32) {
	ret = make([]float32, len(ids))
	for i, v := range ids {
		ret[i] = src[v]
	}

	return
}

func sliceSelectString(src []string, ids []int32) (ret []string) {
	ret = make([]string, len(ids))
	for i, v := range ids {
		ret[i] = src[v]
	}

	return
}

func minOfSlice(vals []float32) (ret float32) {
	ret = vals[0]
	for _, v := range vals {
		if v < ret {
			ret = v
		}
	}

	return
}

func maxOfSlice(vals []float32) (ret float32) {
	ret = vals[0]
	for _, v := range vals {
		if v > ret {
			ret = v
		}
	}

	return
}

func sumOfSlice(vals []float32) (ret float32) {
	for _, val := range vals {
		ret += val
	}

	return
}

func meanOfSlice(vals []float32) (ret float32) {
	return sumOfSlice(vals) / float32(len(vals))
}

func minWithinIds(vals []float32, ids []int32) (rid int32, val float32) {
	rid = ids[0]
	val = vals[ids[0]]

	for _, id := range ids {
		if vals[id] < val {
			val = vals[id]
			rid = id
		}
	}

	return
}

func maxWithinIds(vals []float32, ids []int32) (rid int32, val float32) {
	rid = ids[0]
	val = vals[ids[0]]

	for _, id := range ids {
		if vals[id] > val {
			val = vals[id]
			rid = id
		}
	}

	return
}

func sumWithinIds(vals []float32, ids []int32) (ret float32) {
	for _, id := range ids {
		ret += vals[id]
	}

	return
}

func meanWithinIds(vals []float32, ids []int32) (ret float32) {
	return sumWithinIds(vals, ids) / float32(len(ids))
}

type KeyID struct {
	Key string
	ID  int32
}

func KeyIDSort(s []KeyID) {
	sort.Slice(s, func(i, j int) bool { return s[i].Key < s[j].Key })
}

func merge(s, h []KeyID, mid int) {
	i, j, c := 0, mid, 0
	for i < mid && j < len(s) {
		if s[i].Key < s[j].Key {
			h[c] = s[i]
			c++
			i++
		} else {
			h[c] = s[j]
			c++
			j++
		}
	}

	if i < mid {
		copy(h[c:], s[i:mid])
	} else if j < len(s) {
		copy(h[c:], s[j:])
	}

	copy(s, h)
}

func ParallelSort(s []KeyID) {
	h := make([]KeyID, len(s))
	parallelSort(s, h)
}

func parallelSort(s, h []KeyID) {
	len := len(s)

	if len > 1 {
		if len <= SortMax { // Sequential
			KeyIDSort(s)
		} else { // Parallel
			middle := len / 2

			var wg sync.WaitGroup
			wg.Add(1)

			go func() {
				defer wg.Done()
				parallelSort(s[:middle], h[:middle])
			}()

			parallelSort(s[middle:], h[middle:])

			wg.Wait()
			merge(s, h, middle)
		}
	}
}

func compareFuncs(f1, f2 interface{}) bool {
	return fmt.Sprintf("%v", f1) == fmt.Sprintf("%v", f2)
}

func compareColumns(f1, f2 map[string]ColEntry) bool {
	if len(f1) != len(f2) {
		return false
	}

	for k := range f1 {
		if _, suc := f2[k]; !suc {
			return false
		}
	}

	return true
}

func copyFloat32Slice(s []float32) (ret []float32) {
	ret = make([]float32, len(s))
	copy(ret, s)
	return
}

func copyStringSlice(s []string) (ret []string) {
	ret = make([]string, len(s))
	copy(ret, s)
	return
}
