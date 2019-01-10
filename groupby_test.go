package gframe

import (
	"encoding/json"
	"runtime/debug"
	"sort"
	"sync"
	"testing"
)

var comDF *DataFrame
var initOnceGroupBy sync.Once

func initComDF() {
	comDF = &DataFrame{}
	comDF.createWithData(
		map[string]interface{}{
			"a": []string{"g1", "g2", "g3", "g1", "g2", "g3", "g1", "g3"},
			"b": []float32{1, 2, 3, 4, 5, 6, 7, 8},
			"c": []float32{15, 16, 17, 18, 19, 20, 21, 22},
		},
	)
}

func TestDataFrameGroupby_buildFromDF(t *testing.T) {
	defer func() {
		stack := debug.Stack()
		if err := recover(); err != nil {
			t.Fatalf("error panics %v, stack = %s", err, stack)
		}
	}()

	d := &DataFrame{}
	d.createWithData(
		map[string]interface{}{
			"a": []string{"g1", "g2", "g3", "g1", "g2", "g3", "g1", "g3"},
			"b": []float32{1, 2, 3, 4, 5, 6, 7, 8},
			"c": []float32{0, 1, 0, 1, 0, 1, 0, 1},
		},
	)

	g1 := &DataFrameWithGroupBy{}
	if err := g1.buildFromDFMR(d, []string{"j"}); err == nil {
		t.Errorf("nil error when building with magic columns")
	}

	g2 := &DataFrameWithGroupBy{}
	if err := g2.buildFromDFMR(d, []string{"b"}); err == nil {
		t.Errorf("nil error when building with id columns")
	}

	t.Logf("d.shape = %v", d.shape)
	g3 := d.GroupBy("a")

	if len(g3.groups) != 3 {
		t.Errorf("incorrect group count")
	}

	if len(g3.groupMap["g1"].ids) != 3 {
		t.Errorf("incorrect group size for g1")
	}

	ids := []int32{0, 0, 0}
	copy(ids, g3.groupMap["g1"].ids)
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	if ids[1] != 3 {
		t.Errorf("incorrect group id for g1, ids = %v", ids)
	}

	jd, _ := json.Marshal(g3.groupMap)
	t.Logf("group map = %s", jd)
}

func TestDataFrameGroupby_Aggregate(t *testing.T) {
	defer func() {
		stack := debug.Stack()
		if err := recover(); err != nil {
			t.Fatalf("error panics %v, stack = %s", err, stack)
		}
	}()

	initOnceGroupBy.Do(initComDF)
	g := comDF.GroupBy("a")
	res := g.Aggregate(map[string]interface{}{
		"b": ReduceMin,
		"c": ReduceMax,
	})

	if len(res.cols) != 3 {
		t.Errorf("invalid columns size [%d/%d]", len(res.cols), 3)
	}

	if res.shape[0] != 3 {
		t.Errorf("invalid result shape")
	}

	minCheckB := map[string]float32{
		"g1": 1,
		"g2": 2,
		"g3": 3,
	}

	groupsA, err := res.getIdCols("a")
	if err != nil {
		t.Fatalf("error in fetch groups a, %v", err)
	}

	groups := groupsA[0]
	mins, _ := res.getValCols("b", "c")
	for i := 0; i < len(mins[0]); i++ {
		if minCheckB[groups[i]] != mins[0][i] {
			t.Errorf("incorrect min value for column B, [%f / %f]", minCheckB[groups[i]], mins[0][i])
		}
	}

	minCheckC := map[string]float32{
		"g1": 21,
		"g2": 19,
		"g3": 22,
	}

	for i := 0; i < len(mins[1]); i++ {
		if minCheckC[groups[i]] != mins[1][i] {
			t.Errorf("incorrect min value for column C, [%f / %f]", minCheckC[groups[i]], mins[1][i])
		}
	}
}

func TestDataFrameGroupby_Rank_panic_if_id_column(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Fatalf("no panic when id column is passed in")
		}
	}()

	g := comDF.GroupBy("a")
	_ = g.Rank(false, []string{"a"}, "_rnk", false)
}

func TestDataFrameGroupby_Rank_consistency_check(t *testing.T) {
	defer func() {
		stack := debug.Stack()
		if err := recover(); err != nil {
			t.Fatalf("error panics %v, stack = %s", err, stack)
		}
	}()

	rankBGroudtruth := []float32{
		1, 1, 1, 2, 2, 2, 3, 3,
	}

	g := comDF.GroupBy("a")
	rank := g.Rank(false, []string{"b", "c"}, "_rnk", false)
	if rank.shape[1] != 2 {
		t.Errorf("inconsistent shape of rank result")
	}

	cmp, _ := rank.getValCols("b")
	for i := 0; i < len(cmp); i++ {
		if cmp[0][i] != rankBGroudtruth[i] {
			t.Errorf("inconsistent value of rank %d[%f/%f]", i, cmp[0][i], rankBGroudtruth[i])
		}
	}
}

func TestDataFrameWithGroupby_LeftMerge(t *testing.T) {
	initOnceGroupBy.Do(initComDF)

	right, _ := CreateByData(map[string]interface{}{
		"a": []string{"g1", "g2", "g3"},
		"c": []float32{1, 5, 7},
		"d": []float32{10, 20, 30},
	})

	merged := comDF.LeftMerge(right, []string{"a"}, []string{"c", "d"}, "_a", true)
	merged.Show()

	right2, _ := CreateByData(map[string]interface{}{
		"a": []string{"g1", "g2", "g3"},
		"c": []float32{1, 5, 7},
		"d": []float32{10, 20, 30},
	})

	merged2 := comDF.LeftMerge(right2, []string{"a"}, []string{"c", "d"}, "_a", false)
	merged2.Show()

	right3, _ := CreateByData(map[string]interface{}{
		"a": []string{"g1", "g1", "g2", "g2", "g3", "g3"},
		"c": []float32{1, 1, 5, 5, 7, 7},
		"d": []float32{10, 20, 30, 40, 50, 60},
	})

	merged3 := comDF.LeftMerge(right3, []string{"a"}, []string{"c", "d"}, "_a", false)
	merged3.Show()
}

func TestDataFrameWithGroupby_FindOrder(t *testing.T) {
	tg, _ := CreateByData(map[string]interface{}{
		"a": []string{"g1", "g2", "g3", "g1", "g2", "g3"},
		"b": []float32{4.1, 2, 3, 4, 1, 10},
		"c": []float32{15, 19, 17, 18, 16, 20},
	})

	gSettings.ThreadNum = 1
	initOnceGroupBy.Do(initComDF)
	order := comDF.GroupBy("a").FindOrder(tg.GroupBy("a"), []string{"b", "c"}, false, "_rnk", true)
	if order == nil {
		t.Fatalf("nil return of find order")
	}

	order.Show()
}
