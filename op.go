package gframe

import (
	"fmt"
	"math"
	"reflect"
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

var ReduceMin = NewBinaryOP(reduceMin, float32(math.MaxFloat32))

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

func checkParamterType(f interface{}, cols []ColEntry) (reterr error) {
	tp := reflect.TypeOf(f)
	if tp.Kind() != reflect.Func {
		reterr = fmt.Errorf("the given processor is not a function")
		return
	}

	if tp.NumIn() != len(cols) {
		reterr = fmt.Errorf("paramter list are of different length of columns")
		return
	}

	var errList []string
	for i := 0; i < tp.NumIn(); i++ {
		if cols[i].tp == String {
			if tp.In(i).Kind() != reflect.String {
				errList = append(errList, cols[i].Name)
			}
		} else if cols[i].tp == Float32 {
			if tp.In(i).Kind() != reflect.Float32 {
				errList = append(errList, cols[i].Name)
			}
		} else {
			reterr = fmt.Errorf("unacceptable type of column %s", cols[i].Name)
			return
		}
	}

	if len(errList) > 0 {
		reterr = fmt.Errorf("type of columns %v are not aligned with given function paramters", errList)
	}

	return
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

func checkReturnType(f interface{}, cols []ColEntry) (withErr bool, reterr error) {
	tp := reflect.TypeOf(f)
	if tp.Kind() != reflect.Func {
		reterr = fmt.Errorf("the given processor is not a function")
		return
	}

	if tp.NumOut() == 0 {
		reterr = fmt.Errorf("the processor function is without any output")
		return
	}

	if tp.Out(tp.NumOut()-1).Kind() == reflect.Interface &&
		tp.Out(tp.NumOut()-1).Implements(errorInterface) {
		withErr = true
	}

	d := tp.NumOut() - len(cols)
	if withErr {
		d--
	}

	if d != 0 {
		reterr = fmt.Errorf("the columns' length are not aligned with given function's output")
		return
	}

	var errList []string
	for i := 0; i < tp.NumOut(); i++ {
		if cols[i].tp == String {
			if tp.Out(i).Kind() != reflect.String {
				errList = append(errList, cols[i].Name)
			}
		} else if cols[i].tp == Float32 {
			if tp.Out(i).Kind() != reflect.Float32 {
				errList = append(errList, cols[i].Name)
			}
		} else if cols[i].tp == Unknown {
			if tp.Out(i).Kind() == reflect.Float32 {
				cols[i].tp = Float32
			} else if tp.Out(i).Kind() == reflect.String {
				cols[i].tp = String
			} else {
				errList = append(errList, cols[i].Name)
			}
		} else {
			reterr = fmt.Errorf("unacceptable type of column %s", cols[i].Name)
			return
		}
	}

	if len(errList) > 0 {
		reterr = fmt.Errorf("type of columns %v are not aligned with given function outputs", errList)
	}

	return
}
