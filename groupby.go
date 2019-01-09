package gframe

import (
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	mm "github.com/spaolacci/murmur3"
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
	max *= 1.1
	a.binStride = (max - min) / float32(binSize)

	for _, id := range ids {
		l := a.locate(col[id])
		if l < 0 {
			l = 0
		}

		if int(l) >= len(a.bins) {
			Log("Invalid l value, over bound, l = %d, val = %f, min = %f, max = %f", l, col[id], a.min, a.max)
		}
		a.bins[l] = append(a.bins[l], id)
	}

	a.integral = make([]int32, len(a.bins)+1)
	inte := int32(0)
	for i, b := range a.bins {
		a.integral[i] = inte
		inte += int32(len(b))
		sort.Slice(b, func(i, j int) bool { return col[b[i]] < col[b[j]] })
	}
	a.integral[len(a.bins)] = inte

	return
}

func (a *AugHistogram) locate(v float32) (ret int32) {
	ret = int32((v - a.min) / a.binStride)
	return
}

func (a *AugHistogram) findOrder(v float32, col []float32) (ret int32) {
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

func (a *AugHistogram) fillOrders(pct bool, out []float32) {
	cnt := 0
	all := a.integral[len(a.integral)-1]
	dm := float32(all - 1)
	if all <= 1 {
		dm = 1
	}

	proc := func(v float32) float32 {
		return 1 - v/dm
	}
	if !pct {
		proc = func(v float32) float32 {
			return v
		}
	}

	for _, bin := range a.bins {
		for _, b := range bin {
			out[b] = proc(float32(b))
			cnt++
		}
	}

	return
}

func (a *AugHistogram) getSortedId() (ret []int32) {
	cnt := 0
	ret = make([]int32, a.integral[len(a.integral)-1])
	for _, bin := range a.bins {
		for _, b := range bin {
			ret[cnt] = b
			cnt++
		}
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

func (g *GroupEntry) getKeyStr() string {
	return strings.Join(g.keys, IDMergeDelim)
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

func (d *DataFrameWithGroupBy) getKeyIDTuples(keyCols []string) (ret []KeyID, reterr error) {
	kc, reterr := d.getIdCols(keyCols...)
	if reterr != nil {
		return
	}

	buf := make([]string, len(keyCols))
	for i := 0; i < d.shape[0]; i++ {
		for j := 0; j < len(kc); j++ {
			buf[j] = kc[j][i]
		}

		key := strings.Join(buf, IDMergeDelim)
		ret = append(ret, KeyID{
			Key: key,
			ID:  int32(i),
		})
	}

	return
}

func (d *DataFrameWithGroupBy) buildFromDFImpl2(df *DataFrame, keyCols []string) (reterr error) {
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			panic(fmt.Errorf("Error panics: %v, stack = %s", err, stack))
		}
	}()

	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of building groups from dataframe: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	tStartMR := time.Now()

	// shuffers
	nThread := int(gSettings.ThreadNum)
	shufflers := make([]chan KeyID, nThread)
	for i := 0; i < nThread; i++ {
		shufflers[i] = make(chan KeyID, 1000)
	}

	// GenKeyTuples
	go func() {
		Parallel(nThread, func(id int) {
			buf := make([]string, len(keyCols))
			for i := id; i < d.shape[0]; i += int(nThread) {
				key := strings.Join(buf, IDMergeDelim)
				tid := int(mm.Sum32([]byte(key))) % nThread
				shufflers[tid] <- KeyID{key, int32(i)}
			}
		})

		for i := 0; i < nThread; i++ {
			close(shufflers[i])
		}
	}()

	groupCollection := make([]map[string]*GroupEntry, nThread)
	Parallel(nThread, func(id int) {
		groups := make(map[string]*GroupEntry)
		groupCollection[id] = groups
		for v := range shufflers[id] {
			var g *GroupEntry
			if gg, suc := groups[v.Key]; !suc {
				g = &GroupEntry{}
			} else {
				g = gg
			}
			if g.keys == nil {
				g.keys = strings.Split(v.Key, IDMergeDelim)
			}

			g.ids = append(g.ids, v.ID)
			groups[v.Key] = g
		}
	})

	tEndMR := time.Now()
	if gSettings.Profiling {
		Log("Cost of map reduce: %fms", tEndMR.Sub(tStartMR).Seconds()*1000)
	}

	tStartMerge := time.Now()
	amt := 0
	for _, gp := range groupCollection {
		amt += len(gp)
	}

	d.groups = make([]*GroupEntry, 0, amt)
	d.groupMap = make(map[string]*GroupEntry)
	for _, gp := range groupCollection {
		for k, v := range gp {
			d.groupMap[k] = v
			d.groups = append(d.groups, v)
		}
	}
	tEndMerge := time.Now()
	if gSettings.Profiling {
		Log("Cost of merging groups: %fms", tEndMerge.Sub(tStartMerge).Seconds()*1000)
	}

	tStartHist := time.Now()
	reterr = d.buildHistogram(d.getValueColumnNames())
	tEndHist := time.Now()
	if gSettings.Profiling {
		Log("Cost of building histogram: %fms", tEndHist.Sub(tStartHist).Seconds()*1000)
	}

	return
}

func (d *DataFrameWithGroupBy) buildFromDF(df *DataFrame, keyCols []string) (reterr error) {
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			panic(fmt.Errorf("Error panics: %v, stack = %s", err, stack))
		}
	}()

	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of building groups from dataframe: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	d.DataFrame = *df
	d.keyCols = make([]string, len(keyCols))
	copy(d.keyCols, keyCols)
	kids, reterr := d.getKeyIDTuples(keyCols)
	if reterr != nil {
		return
	}

	tStartSort := time.Now()
	d.groupMap = make(map[string]*GroupEntry)
	ParallelSort(kids)
	tEndSort := time.Now()
	if gSettings.Profiling {
		Log("Cost of parallel sorting in building groups: %fms", tEndSort.Sub(tStartSort).Seconds()*1000)
	}

	tStartBuild := time.Now()
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

	tEndBuild := time.Now()
	if gSettings.Profiling {
		Log("Cost of building groups struct: %fms", tEndBuild.Sub(tStartBuild).Seconds()*1000)
	}

	tStartHist := time.Now()
	reterr = d.buildHistogram(d.getValueColumnNames())
	tEndHist := time.Now()
	if gSettings.Profiling {
		Log("Cost of building histogram: %fms", tEndHist.Sub(tStartHist).Seconds()*1000)
	}

	return
}

func (d *DataFrameWithGroupBy) buildHistogram(cols []string) (reterr error) {
	d.histCols = make(map[string]ColEntry)
	colVals, reterr := d.getValCols(cols...)
	if reterr != nil {
		return
	}

	nThread := gSettings.ThreadNum
	Parallel(int(nThread), func(id int) {
		for i := id; i < len(d.groups); i += int(nThread) {
			func(i int) {
				/*defer func() {
					stack := debug.Stack()
					if err := recover(); err != nil {
						Log("Error panics in No.%d group, err = %v, stack = %s", i, err, stack)
					}
				}()*/

				d.groups[i].buildHistogram(colVals, gSettings.HistogramBins)
			}(i)
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
				rs := d.applyEachGroupID(ent, op.Op)
				rid := ret.colMap[op.col+op.Surfix].id
				ret.idCols[rid] = rs
			} else if ent.tp == Float32 {
				rs := d.applyEachGroupVal(ent, op.Op)
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

func (d *DataFrameWithGroupBy) Rank(pct bool, cols []string, suffix string) (ret *DataFrame) {
	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of ranking: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	histIds := make([]int32, len(cols))
	for i, c := range cols {
		cl, suc := d.histCols[c]
		if !suc {
			panic(fmt.Errorf("column [%s] does not have histogram, is that an Id column?", c))
		}

		histIds[i] = cl.id
	}

	rankCols := make([][]float32, len(cols))
	for i := range rankCols {
		rankCols[i] = make([]float32, d.shape[0])
	}

	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < len(rankCols); i += int(gSettings.ThreadNum) {
			hid := histIds[i]
			for _, g := range d.groups {
				g.hists[hid].fillOrders(pct, rankCols[i])
			}
		}
	})

	ret = Empty()
	for i, c := range cols {
		ret.PasteValColumn(c+suffix, rankCols[i])
	}

	return
}

type mappedEntry struct {
	ColEntry
	srcIDLeft   int32
	srcIDRight  int32
	histIDLeft  int32
	histIDRight int32
}

func (d *DataFrameWithGroupBy) LeftMerge(t *DataFrameWithGroupBy, suffix string, firstMatchOnly bool) (ret *DataFrame) {
	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of merge: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	if !StrSeqEqual(d.keyCols, t.keyCols) {
		panic(fmt.Errorf("unable to merge two groupby frame with different key columns"))
	}

	onSet := make(map[string]struct{})
	for _, v := range d.keyCols {
		onSet[v] = struct{}{}
	}

	nh := d.shape[0]
	groupOffset := make(map[string]int32)
	var availableKeys []string
	if !firstMatchOnly {
		offset := int32(0)
		for k, g := range d.groupMap {
			if tg, suc := t.groupMap[k]; suc {
				groupOffset[k] = offset
				offset += int32(len(g.ids) * len(tg.ids))
				availableKeys = append(availableKeys, k)
			}
		}

		nh = int(offset)
	}

	ret = &DataFrame{}
	ret.reset()
	var mergeMapper []mappedEntry
	for _, col := range d.cols {
		ent, _ := ret.addColumn(col.Name, col.tp == String, false)
		mp := mappedEntry{
			ColEntry:   ent,
			srcIDLeft:  col.id,
			srcIDRight: -1,
		}

		mergeMapper = append(mergeMapper, mp)
	}

	for _, col := range t.cols {
		if _, suc := onSet[col.Name]; suc {
			continue
		}

		var ent ColEntry
		var err error
		if ent, err = ret.addColumn(col.Name, col.tp == String, false); err != nil {
			ent, _ = ret.addColumn(col.Name+suffix, col.tp == String, false)
		}

		mp := mappedEntry{
			ColEntry:   ent,
			srcIDLeft:  -1,
			srcIDRight: col.id,
		}

		mergeMapper = append(mergeMapper, mp)
	}

	ret.alloc(nh)
	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < len(d.groups); i += int(gSettings.ThreadNum) {
			gd := d.groups[i]
			k := gd.getKeyStr()
			gt, suc := t.groupMap[k]
			if !suc {
				continue
			}

			var idLeft, idRight int32
			var srid *int32
			for _, col := range mergeMapper {
				var src *DataFrameWithGroupBy
				var scid int32
				if col.srcIDLeft >= 0 {
					src = d
					scid = col.srcIDLeft
					srid = &idLeft
				} else {
					src = t
					scid = col.srcIDRight
					srid = &idRight
				}

				var assign func(tid, sid int32)
				if col.tp == String {
					rsCol := ret.idCols[col.id]
					sCol := src.idCols[scid]
					assign = func(tid, sid int32) {
						if len(rsCol) <= int(tid) {
							Log("len(rsCol) [%d] <= tid [%d]", len(rsCol), tid)
						}

						if len(sCol) <= int(sid) {
							Log("len(sCol) [%d] <= sid [%d]", len(sCol), sid)
						}

						rsCol[tid] = sCol[sid]
					}
				} else {
					rsCol := ret.valCols[col.id]
					sCol := src.valCols[scid]
					assign = func(tid, sid int32) {
						if len(rsCol) <= int(tid) {
							Log("len(rsCol) [%d] <= tid [%d]", len(rsCol), tid)
						}

						if len(sCol) <= int(sid) {
							Log("len(sCol) [%d] <= sid [%d]", len(sCol), sid)
						}

						rsCol[tid] = sCol[sid]
					}
				}

				if firstMatchOnly {
					idRight = gt.ids[0]
					for _, idLeft = range gd.ids {
						assign(idLeft, *srid)
					}
				} else {
					offset := groupOffset[k]
					for _, idLeft = range gd.ids {
						for _, idRight = range gt.ids {
							assign(offset, *srid)
							offset++
						}
					}
				}
			}
		}
	})

	return
}

func (d *DataFrameWithGroupBy) FindOrder(t *DataFrameWithGroupBy, cols []string, pct bool, suffix string, keepKeyCols bool) (ret *DataFrame) {
	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of finding order: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	var reterr error
	defer func() {
		if reterr != nil {
			panic(reterr)
		}

		if keepKeyCols && ret != nil {
			ret = t.SelectByColumns(d.keyCols...).Copy(true).Concatenate(ret, false)
		}
	}()

	if suc, missing := d.haveColumns(cols); !suc {
		reterr = fmt.Errorf("columns %v are missing from left dataframe", missing)
		return
	}

	if suc, missing := t.haveColumns(cols); !suc {
		reterr = fmt.Errorf("columns %v are missing from right dataframe", missing)
		return
	}

	if _, amb, bma := CompareStrList(d.keyCols, t.keyCols); len(amb) > 0 || len(bma) > 0 {
		reterr = fmt.Errorf("cannot compare two group by with different keys")
		return
	}

	ret = &DataFrame{}
	ret.reset()
	var mapping []mappedEntry
	for _, c := range cols {
		entd := d.colMap[c]
		entt := t.colMap[c]
		hd := d.histCols[c]

		if entt.tp == String {
			reterr = fmt.Errorf("cannot find order for id column: %s", c)
			return
		}

		entr, err := ret.addColumn(c+suffix, entd.tp == String, false)
		if err != nil {
			panic(err)
		}

		mp := mappedEntry{
			ColEntry:   entr,
			srcIDLeft:  entd.id,
			srcIDRight: entt.id,
			histIDLeft: hd.id,
		}

		mapping = append(mapping, mp)
	}

	Log("allocatingg with size %d", t.shape[0])
	ret.alloc(t.shape[0])

	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < len(t.groups); i += int(gSettings.ThreadNum) {
			tg := t.groups[i]
			k := tg.getKeyStr()
			dg, suc := d.groupMap[k]
			if !suc {
				continue
			}

			for _, rid := range tg.ids {
				for _, mp := range mapping {
					order := dg.hists[mp.histIDLeft].findOrder(t.valCols[mp.srcIDRight][rid], d.valCols[mp.srcIDLeft])
					orderF := float32(order)
					if pct {
						orderF = 1 - orderF/float32(len(dg.ids)-1)
					}

					ret.valCols[mp.id][rid] = orderF
				}
			}
		}
	})

	return
}
