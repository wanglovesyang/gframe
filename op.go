package gframe

import (
	"math"
	"sort"
)

type BinaryOP interface {
	Operate(a, b float32) float32
	InitValue() float32
}

type ListOP func(s []float32) float32
type IDListOP func(s []string) string
type BinaryOPFunc func(a, b float32) float32

type BinaryOPBase struct {
	op      BinaryOPFunc
	initVal float32
}

func (p *BinaryOPBase) Operate(a, b float32) float32 {
	return p.op(a, b)
}

func (p *BinaryOPBase) InitValue() float32 {
	return p.initVal
}

func NewBinaryOP(f BinaryOPFunc, initVal float32) *BinaryOPBase {
	return &BinaryOPBase{
		op:      f,
		initVal: initVal,
	}
}

var ReduceSum = NewBinaryOP(reduceSum, float32(0.0))

func reduceSum(a, b float32) float32 {
	return a + b
}

var ReduceMax = NewBinaryOP(reduceMax, -float32(math.MaxFloat32))

func reduceMax(a, b float32) float32 {
	if a > b {
		return a
	}

	return b
}

var ReduceMin = NewBinaryOP(reduceMin, -float32(math.MaxFloat32))

func reduceMin(a, b float32) float32 {
	if a > b {
		return b
	}

	return a
}

func ReduceMean(s []float32) (ret float32) {
	for _, ss := range s {
		ret += ss
	}

	return ret / float32(len(s))
}

func ReduceCentroid(s []float32) (ret float32) {
	rs := make([]float32, len(s))
	copy(rs, s)
	sort.Slice(rs, func(i, j int) bool { return rs[i] < rs[j] })
	return rs[len(rs)/2]
}

func ReduceFirst(s []string) (ret string) {
	return s[0]
}

func ReduceMostCommon(s []string) (ret string) {
	hist := map[string]int32{}
	for _, ss := range s {
		cnt := hist[ss]
		cnt++
		hist[ss] = cnt
	}

	maxCnt := int32(0)
	for k, v := range hist {
		if maxCnt < v {
			maxCnt = v
			ret = k
		}
	}

	return
}

type OperationTuple struct {
	col    string
	Surfix string
	Op     interface{}
}

func validOperation(op interface{}) bool {
	if _, suc := op.(BinaryOP); suc {
		return true
	}

	if _, suc := op.(ListOP); suc {
		return true
	}

	if _, suc := op.(IDListOP); suc {
		return true
	}

	return false
}
