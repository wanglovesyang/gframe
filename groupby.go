package gframe

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

type AugHistogram struct {
	binStride float32
	min       float32
	max       float32
	integral  []int32
	bins      [][]int32
}

func (a *AugHistogram) build(ids []int32, col []float32, binSize int32) {
	min, max := float32(math.MaxFloat32), float32(-math.MaxFloat32)
	for _, id := range ids {
		if col[id] < min {
			min = col[id]
		}

		if col[id] > max {
			max = col[id]
		}
	}

	a.bins = make([][]int32, binSize)
	a.min = min
	a.max = max
	max += 0.000001
	a.binStride = (max - min) / float32(binSize)

	for _, id := range ids {
		l := a.locate(col[id])
		a.bins[l] = append(a.bins[l], id)
	}

	inte := int32(0)
	for i, b := range a.bins {
		a.integral[i] = inte
		inte += int32(len(b))
		sort.Slice(b, func(i, j int) bool { return col[b[i]] < col[b[j]] })
	}

	return
}

func (a *AugHistogram) locate(v float32) (ret int32) {
	ret = int32((v - a.min) / a.binStride)
	return
}

func (a *AugHistogram) order(v float32, col []float32) (ret int32) {
	loc := a.locate(v)
	l := int32(len(a.bins))
	if loc < 0 {
		ret = 0
	} else if loc >= l {
		ret = a.integral[l]
	} else {
		ret = a.integral[loc] + int32(sort.Search(len(a.bins[loc]), func(i int) bool { return v < col[a.bins[loc][i]] }))
	}

	return
}

//-------------------------------------------------------
type GroupEntry struct {
	keys  []string
	ids   []int32
	hists []AugHistogram
	//histCols []string
}

func (g *GroupEntry) buildHistogram(cols [][]float32, binSize int32) {
	g.hists = make([]AugHistogram, len(cols))
	for i, col := range cols {
		g.hists[i].build(g.ids, col, binSize)
	}
}

type DataFrameWithGroupBy struct {
	DataFrame
	keyCols []string

	//histograms
	histCols map[string]ColEntry

	//entries
	groups   []*GroupEntry
	groupMap map[string]*GroupEntry
}

func (d *DataFrameWithGroupBy) buildFromDF(df *DataFrame, keyCols []string) (reterr error) {
	d.DataFrame = *df
	d.keyCols = make([]string, len(keyCols))
	copy(d.keyCols, keyCols)
	kids, reterr := d.getKeyIDTuples(keyCols)
	if reterr != nil {
		return
	}

	d.groupMap = make(map[string]*GroupEntry)
	ParallelSort(kids)

	curKey := ""
	var ids []int32
	for _, kd := range kids {
		if curKey != kd.Key && curKey != "" {
			group := &GroupEntry{
				ids:  ids,
				keys: strings.Split(curKey, IDMergeDelim),
			}

			d.groupMap[curKey] = group
			d.groups = append(d.groups, group)
			ids = nil
		}

		ids = append(ids, kd.ID)
		curKey = kd.Key
	}

	if len(ids) > 0 {
		group := &GroupEntry{
			ids:  ids,
			keys: strings.Split(curKey, IDMergeDelim),
		}

		d.groupMap[curKey] = group
		d.groups = append(d.groups, group)
	}

	d.buildHistogram(d.getValueColumnNames())
	return
}

func (d *DataFrameWithGroupBy) buildHistogram(cols []string) (reterr error) {
	d.histCols = make(map[string]ColEntry)
	colVals, reterr := d.getValCols(cols)
	if reterr != nil {
		return
	}

	nThread := gSettings.ThreadNum
	Parallel(int(nThread), func(id int) {
		for i := id; i < len(d.groups); i += int(nThread) {
			d.groups[i].buildHistogram(colVals, gSettings.ThreadNum)
		}
	})

	for i, col := range cols {
		ent := ColEntry{
			Name: col,
			tp:   Float32,
			id:   int32(i),
		}

		d.histCols[col] = ent
	}

	return
}

func flatternOpList(ops map[string]interface{}) (ret []OperationTuple) {
	for k, v := range ops {
		switch a := v.(type) {
		case OperationTuple:
			a.col = k
			ret = append(ret, a)
		case *OperationTuple:
			b := *a
			b.col = k
			ret = append(ret, b)
		case []OperationTuple:
			for _, p := range a {
				p.col = k
				ret = append(ret, p)
			}
		case []*OperationTuple:
			for _, p := range a {
				pp := *p
				pp.col = k
				ret = append(ret, pp)
			}
		default:
			ret = append(ret, OperationTuple{
				col:    k,
				Surfix: "",
				Op:     a,
			})
		}
	}

	return
}

func (d *DataFrameWithGroupBy) fastApply(ent ColEntry, op interface{}) (ret []float32, suc bool) {
	cid := d.histCols[ent.Name].id
	ret = make([]float32, len(d.groups))
	if op.(BinaryOP) == ReduceMax {
		for i, g := range d.groups {
			ret[i] = g.hists[cid].max
		}
	} else if op.(BinaryOP) == ReduceMin {
		for i, g := range d.groups {
			ret[i] = g.hists[cid].min
		}
	} else {
		suc = false
	}

	suc = true
	return
}

func (d *DataFrameWithGroupBy) applyEachGroupVal(ent ColEntry, op interface{}) (ret []float32) {
	var suc bool
	if ret, suc = d.fastApply(ent, op); suc {
		return
	}

	ret = make([]float32, len(d.groups))
	cid := ent.id
	switch p := op.(type) {
	case BinaryOP:
		colVal := d.valCols[cid]
		for i, g := range d.groups {
			base := p.InitValue()
			for _, id := range g.ids {
				base = p.Operate(base, colVal[id])
			}
			ret[i] = base
		}

	case ListOP:
		colVal := d.valCols[cid]
		var buf []float32
		for i, g := range d.groups {
			if len(buf) < len(g.ids) {
				buf = append(buf, make([]float32, len(g.ids)-len(buf))...)
			}

			for j, id := range g.ids {
				buf[j] = colVal[id]
			}

			ret[i] = p(buf)
		}
	default:
		panic(fmt.Errorf("This type of operation cannot be adopted in value columns"))
	}

	return
}

func (d *DataFrameWithGroupBy) applyEachGroupID(ent ColEntry, op interface{}) (ret []string) {
	ret = make([]string, len(d.groups))
	cid := ent.id
	switch p := op.(type) {
	case IDListOP:
		idVal := d.idCols[cid]
		var buf []string
		for i, g := range d.groups {
			if len(buf) < len(g.ids) {
				buf = append(buf, make([]string, len(g.ids)-len(buf))...)
			}

			for j, id := range g.ids {
				buf[j] = idVal[id]
			}

			ret[i] = p(buf)
		}
	default:
		panic(fmt.Errorf("This type of operation cannot be adopted in value columns"))
	}

	return
}

func (d *DataFrameWithGroupBy) ApplyEachGroup(ops map[string]interface{}) (ret *DataFrame) {
	opList := flatternOpList(ops)
	for _, op := range opList {
		if !validOperation(op.Op) {
			panic(fmt.Errorf("invalid type of operation for column: %s", op.col))
		}
	}

	ret = &DataFrame{}
	colInfo := make(map[string]int)
	for _, col := range d.keyCols {
		colInfo[col] = String
	}

	for _, op := range opList {
		if _, suc := colInfo[op.col]; suc {
			panic(fmt.Errorf("column %s is already in group key", op.col))
		}

		colInfo[op.col+op.Surfix] = int(d.colMap[op.col].tp)
	}

	ret.registerColumns(colInfo)
	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < len(opList); i += int(gSettings.ThreadNum) {
			op := opList[i]
			ent := d.colMap[op.col]
			if ent.tp == String {
				rs := d.applyEachGroupID(ent, op)
				rid := ret.colMap[op.col+op.Surfix].id
				ret.idCols[rid] = rs
			} else if ent.tp == Float32 {
				rs := d.applyEachGroupVal(ent, op)
				rid := ret.colMap[op.col+op.Surfix].id
				ret.valCols[rid] = rs
			}
		}
	})

	// Adding keys
	for j, name := range d.keyCols {
		col := make([]string, len(d.groups))
		for i, g := range d.groups {
			col[i] = g.keys[j]
		}

		cid := ret.colMap[name].id
		ret.idCols[cid] = col
	}

	ret.shape[1] = len(d.groups)
	ret.shape[0] = len(ret.cols)

	return
}

func (d *DataFrameWithGroupBy) Rank(cols []string, suffix string, inplace bool) (ret *DataFrame) {

}
