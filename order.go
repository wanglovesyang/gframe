package gframe

import (
	"math"
	"sort"
)

type OrderGroup interface {
	OrderedIDs() []int32
	FindOrder(v float32, col []float32) int32
	Min(col []float32) (float32, int32)
	Max(col []float32) (float32, int32)
}

func NewOrderGroup(col []float32, ids []int32) (ret OrderGroup) {
	if len(ids) > int(gSettings.orderGroupThreshold) {
		ret = NewOrderGroupWithBins(col, ids)
	} else {
		ret = NewSimpleOrderGroup(col, ids)
	}

	return ret
}

type SimpleOrderGroup []int32

func NewSimpleOrderGroup(col []float32, ids []int32) (ret SimpleOrderGroup) {
	rets := make([]int32, len(ids))
	for i, id := range ids {
		rets[i] = id
	}

	sort.Slice(rets, func(i, j int) bool {
		return col[rets[i]] < col[rets[j]]
	})
	return rets
}

func (s SimpleOrderGroup) OrderedIDs() []int32 {
	return s
}

func (s SimpleOrderGroup) FindOrder(v float32, col []float32) int32 {
	return int32(sort.Search(len(s), func(i int) bool { return v < col[s[i]] }))
}

func (s SimpleOrderGroup) Min(col []float32) (v float32, f int32) {
	f = s[0]
	v = col[f]
	return
}

func (s SimpleOrderGroup) Max(col []float32) (v float32, f int32) {
	f = s[len(s)-1]
	v = col[f]
	return
}

type OrderGroupWithBins struct {
	order     []int32
	binStride float32
	offsets   []int32
}

func NewOrderGroupWithBins(col []float32, ids []int32) (ret *OrderGroupWithBins) {
	min, max := float32(math.MaxFloat32), float32(-math.MaxFloat32)
	for _, id := range ids {
		if col[id] < min {
			min = col[id]
		}

		if col[id] > max {
			max = col[id]
		}
	}

	ret = &OrderGroupWithBins{}
	ret.binStride = (max - min) / float32(gSettings.OrderBins)
	ret.order = make([]int32, len(ids))
	copy(ret.order, ids)
	if ret.binStride == 0 {
		return
	}
	sort.Slice(ret.order, func(i, j int) bool { return col[ret.order[i]] < col[ret.order[j]] })

	ret.offsets = make([]int32, gSettings.OrderBins)
	for i := range ret.offsets {
		ret.offsets[i] = int32(len(ret.order))
	}

	for i := 0; i < len(ret.order); i++ {
		l := ret.locate(col[ret.order[i]], col)
		if l >= 0 && l < int32(len(ret.offsets)) {
			if ret.offsets[l] > int32(i) {
				ret.offsets[l] = int32(i)
			}
		}
	}

	return
}

func (o *OrderGroupWithBins) OrderedIDs() []int32 {
	return o.order
}

func (o *OrderGroupWithBins) locate(v float32, col []float32) (ret int32) {
	ret = int32((v - col[o.order[0]]) / o.binStride)
	return
}

func (o *OrderGroupWithBins) FindOrder(v float32, col []float32) int32 {
	binId := o.locate(v, col)
	if int(binId) >= len(o.offsets) {
		return int32(len(o.order))
	}

	if binId < 0 {
		return 0
	}

	offset := o.offsets[binId]
	if binId == int32(len(o.offsets)-1) {
		return offset + int32(sort.Search(len(o.order)-int(offset), func(id int) bool { return v < col[id] }))
	} else if offsetNext := o.offsets[binId+1]; offsetNext == offset {
		return offset
	} else {
		return offset + int32(sort.Search(int(-offset), func(id int) bool { return v < col[id] }))
	}
}

func (o *OrderGroupWithBins) Max(col []float32) (v float32, f int32) {
	f = o.order[len(o.order)-1]
	v = col[f]
	return
}

func (o *OrderGroupWithBins) Min(col []float32) (v float32, f int32) {
	f = o.order[0]
	v = col[f]
	return
}
