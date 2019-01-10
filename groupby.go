package gframe

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	mm "github.com/spaolacci/murmur3"
)

//-------------------------------------------------------
type GroupEntry struct {
	keys        []string
	ids         []int32
	orderGroups []OrderGroup
}

func (g *GroupEntry) addOrderGroup(col []float32) {
	g.orderGroups = append(g.orderGroups, NewOrderGroup(col, g.ids))
	return
}

func (g *GroupEntry) getKeyStr() string {
	return strings.Join(g.keys, IDMergeDelim)
}

type DataFrameWithGroupBy struct {
	DataFrame
	keyCols []string

	//histograms
	orderCols   []ColEntry
	orderColMap map[string]ColEntry

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

func (d *DataFrameWithGroupBy) buildFromDFMR(df *DataFrame, keyCols []string) (reterr error) {
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			reterr = fmt.Errorf("Error panics: %v, stack = %s", err, stack)
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
	d.keyCols = keyCols
	d.orderColMap = make(map[string]ColEntry)

	tStartMR := time.Now()
	kc, reterr := df.getIdCols(keyCols...)
	if reterr != nil {
		return
	}

	// shuffers
	nThread := int(gSettings.ThreadNum)
	shufflers := make([]chan KeyID, nThread)
	for i := 0; i < nThread; i++ {
		shufflers[i] = make(chan KeyID, 1000)
	}

	if gSettings.Debug {
		Log("d.shape = %v", df.shape)
	}

	// GenKeyTuples
	go func() {
		tMapStart := time.Now()
		cntH := int32(0)
		Parallel(nThread, true, func(id int) {
			buf := bytes.Buffer{}
			for i := id; i < df.shape[0]; i += int(nThread) {
				for j := 0; j < len(kc)-1; j++ {
					buf.WriteString(kc[j][i])
					buf.WriteByte(IDMergeDelimChar)
				}
				buf.WriteString(kc[len(kc)-1][i])

				key := buf.Bytes()
				tid := int(mm.Sum32(key)) % nThread
				shufflers[tid] <- KeyID{string(key), int32(i)}
				atomic.AddInt32(&cntH, 1)
				buf.Reset()
			}
		})

		for i := 0; i < nThread; i++ {
			close(shufflers[i])
		}

		if gSettings.Debug {
			Log("totally %d rows mapped", cntH)
		}

		tMapEnd := time.Now()
		if gSettings.Profiling {
			Log("Cost of Map: %fms", tMapEnd.Sub(tMapStart).Seconds()*1000)
		}
	}()

	tReduceStart := time.Now()
	groupCollection := make([]map[string]*GroupEntry, nThread)
	rCnt := int32(0)
	Parallel(nThread, true, func(id int) {
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
			atomic.AddInt32(&rCnt, 1)
		}
	})

	tEndMR := time.Now()
	if gSettings.Profiling {
		Log("Cost of Reduce: %fms", tEndMR.Sub(tReduceStart).Seconds()*1000)
		Log("Cost of map reduce: %fms", tEndMR.Sub(tStartMR).Seconds()*1000)
	}

	tStartMerge := time.Now()
	amt := 0
	for _, gp := range groupCollection {
		amt += len(gp)
	}

	if gSettings.Debug {
		Log("totally %d groups", amt)
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

	return
}

func (d *DataFrameWithGroupBy) NumGroups() int32 {
	return int32(len(d.groups))
}

func (d *DataFrameWithGroupBy) BuildOrderGroups(cols []string) (reterr error) {
	var newCols []string
	var ignoreCols []string
	for _, col := range cols {
		if _, suc := d.orderColMap[col]; !suc {
			newCols = append(newCols, col)
		} else {
			ignoreCols = append(ignoreCols, col)
		}
	}

	if len(ignoreCols) > 0 {
		Log("order groups of columns %v have already been built")
	}

	if len(newCols) == 0 {
		return
	}

	colVals, reterr := d.getValCols(newCols...)
	if reterr != nil {
		return
	}

	for _, col := range newCols {
		ent := ColEntry{
			Name: col,
			tp:   Float32,
			id:   int32(len(d.orderCols)),
		}

		d.orderCols = append(d.orderCols, ent)
		d.orderColMap[col] = ent
	}

	nThread := gSettings.ThreadNum
	Parallel(int(nThread), true, func(id int) {
		for i := id; i < len(d.groups); i += int(nThread) {
			func(i int) {
				for _, col := range colVals {
					d.groups[i].addOrderGroup(col)
				}
			}(i)
		}
	})

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

type AggregateApply func(g *GroupEntry) float32
type AggregateApplyID func(g *GroupEntry) string

func (d *DataFrameWithGroupBy) tryFastAggApply(ent ColEntry, op interface{}) (ret AggregateApply, suc bool) {
	c, suc := d.orderColMap[ent.Name]
	if !suc {
		return
	}

	colVal := d.valCols[ent.id]
	if op.(BinaryOP) == ReduceMax {
		ret = func(g *GroupEntry) float32 {
			r, _ := g.orderGroups[c.id].Max(colVal)
			return r
		}
	} else if op.(BinaryOP) == ReduceMin {
		ret = func(g *GroupEntry) float32 {
			r, _ := g.orderGroups[c.id].Min(colVal)
			return r
		}
	} else {
		suc = false
	}

	suc = true
	return
}

func (d *DataFrameWithGroupBy) buildAggApplyVal(ent ColEntry, op interface{}) (ret AggregateApply) {
	var suc bool
	if ret, suc = d.tryFastAggApply(ent, op); suc {
		return
	}

	cid := ent.id
	colVal := d.valCols[cid]
	switch p := op.(type) {
	case BinaryOP:
		ret = func(g *GroupEntry) float32 {
			base := p.InitValue()
			for _, id := range g.ids {
				base = p.Operate(base, colVal[id])
			}
			return base
		}
	case ListOP:
		ret = func(g *GroupEntry) float32 {
			var buf []float32
			if len(buf) < len(g.ids) {
				buf = append(buf, make([]float32, len(g.ids)-len(buf))...)
			}

			for j, id := range g.ids {
				buf[j] = colVal[id]
			}

			return p(buf)
		}
	default:
		panic(fmt.Errorf("This type of operation cannot be adopted in value columns"))
	}

	return
}

func (d *DataFrameWithGroupBy) buildAggApplyID(ent ColEntry, op interface{}) (ret AggregateApplyID) {
	cid := ent.id
	idVal := d.idCols[cid]
	switch p := op.(type) {
	case IDListOP:
		ret = func(g *GroupEntry) string {
			var buf []string
			if len(buf) < len(g.ids) {
				buf = append(buf, make([]string, len(g.ids)-len(buf))...)
			}

			for j, id := range g.ids {
				buf[j] = idVal[id]
			}

			return p(buf)
		}
	default:
		panic(fmt.Errorf("This type of operation cannot be adopted in value columns"))
	}

	return
}

func (d *DataFrameWithGroupBy) Aggregate(ops map[string]interface{}) (ret *DataFrame) {
	opList := flatternOpList(ops)
	for _, op := range opList {
		if !validOperation(op.Op) {
			panic(fmt.Errorf("invalid type of operation for column: %s", op.col))
		}
	}

	ret = Empty()
	for _, col := range d.keyCols {
		ret.addColumn(col, true, false)
	}

	type mapEntWithOps struct {
		mappedEntry
		valAgg AggregateApply
		strAgg AggregateApplyID
	}

	opCols := make([]mapEntWithOps, 0, len(opList)+len(d.keyCols))
	for _, op := range opList {
		if _, suc := ret.colMap[op.col]; suc {
			panic(fmt.Errorf("column %s is already in group key", op.col))
		}

		if c, suc := d.colMap[op.col]; !suc {
			panic(fmt.Errorf("column %s does not exist in frame", op.col))
		} else {
			ent, _ := ret.addColumn(op.col+op.Surfix, c.tp == String, false)
			var mc mapEntWithOps
			if c.tp == String {
				mc.strAgg = d.buildAggApplyID(c, op.Op)
			} else if c.tp == Float32 {
				mc.valAgg = d.buildAggApplyVal(c, op.Op)
			}
			mc.mappedEntry = mappedEntry{
				ColEntry:  ent,
				srcIDLeft: c.id,
			}

			opCols = append(opCols, mc)
		}
	}

	ret.alloc(len(d.groups))
	Parallel(int(gSettings.ThreadNum), true, func(id int) {
		for i := id; i < len(d.groups); i += int(gSettings.ThreadNum) {
			g := d.groups[i]
			//fill keys
			for k := 0; k < len(d.keyCols); k++ {
				ret.idCols[k][i] = g.keys[k]
			}

			//agg
			for _, opc := range opCols {
				if opc.tp == String {
					res := opc.strAgg(g)
					ret.idCols[opc.id][i] = res
				} else if opc.tp == Float32 {
					res := opc.valAgg(g)
					ret.valCols[opc.id][i] = res
				}
			}
		}
	})

	return
}

type RankOps func(g *GroupEntry) []int32

func (d *DataFrameWithGroupBy) buildRankOp(name string) (ret RankOps) {
	if c, suc := d.orderColMap[name]; suc {
		ret = func(g *GroupEntry) []int32 {
			return g.orderGroups[c.id].OrderedIDs()
		}
	} else {
		colVals, err := d.getValCols(name)
		if err != nil {
			panic(err)
		}
		colVal := colVals[0]

		ret = func(g *GroupEntry) (r []int32) {
			r = make([]int32, len(g.ids))
			copy(r, g.ids)
			sort.Slice(r, func(i, j int) bool { return colVal[r[i]] < colVal[r[j]] })
			return
		}
	}

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

	if suc, missing := d.haveColumns(cols); !suc {
		panic(fmt.Errorf("columns: %v does not exist in dataframe", missing))
	}

	// TODO try id columns

	type entWithRankOp struct {
		ColEntry
		op RankOps
	}

	ret = Empty()
	var rankEnts []entWithRankOp
	for _, col := range cols {
		ent, _ := ret.addColumn(col+suffix, false, false)
		op := d.buildRankOp(col)
		rankEnts = append(rankEnts, entWithRankOp{
			ColEntry: ent,
			op:       op,
		})
	}

	ret.alloc(d.shape[0])

	Parallel(int(gSettings.ThreadNum), true, func(id int) {
		for i := id; i < len(d.groups); i += int(gSettings.ThreadNum) {
			g := d.groups[i]
			for _, opc := range rankEnts {
				order := opc.op(g)
				target := ret.valCols[opc.id]
				if len(order) == 1 {
					target[order[0]] = 0
					continue
				}

				if pct {
					d := float32(len(order) - 1)
					for i, r := range order {
						target[r] = 1 - float32(i)/d
					}
				} else {
					for i, r := range order {
						target[r] = float32(i)
					}
				}
			}
		}
	})

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
	Parallel(int(gSettings.ThreadNum), true, func(id int) {
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
	d.BuildOrderGroups(cols)

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
		hd := d.orderColMap[c]

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

	Parallel(int(gSettings.ThreadNum), true, func(id int) {
		for i := id; i < len(t.groups); i += int(gSettings.ThreadNum) {
			tg := t.groups[i]
			k := tg.getKeyStr()
			dg, suc := d.groupMap[k]
			if !suc {
				continue
			}

			for _, rid := range tg.ids {
				for _, mp := range mapping {
					order := dg.orderGroups[mp.histIDLeft].FindOrder(t.valCols[mp.srcIDRight][rid], d.valCols[mp.srcIDLeft])
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
