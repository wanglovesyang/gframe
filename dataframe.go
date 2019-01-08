package gframe

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	Droped  = -2
	Unknown = -1
	String  = iota
	Float32
)

const MaxCSVBufferSize = 10000000
const IDMergeDelim = "\t"

type ColEntry struct {
	Name string
	tp   int8
	id   int32
}

type DataFrame struct {
	// Columns-wise
	cols   []ColEntry
	colMap map[string]ColEntry

	shape   [2]int
	idCols  [][]string
	valCols [][]float32
}

func (d *DataFrame) reset() {
	d.colMap = make(map[string]ColEntry)
	d.cols = nil
	d.shape = [2]int{0, 0}
	d.idCols = nil
	d.valCols = nil
}

func (d *DataFrame) createWithData(data map[string]interface{}) (reterr error) {
	d.reset()
	lastLen := -1
	for k, v := range data {
		switch vv := v.(type) {
		case []float32:
			if ent, err := d.addColumn(k, false, false); err != nil {
				return err
			} else if len(vv) != lastLen && lastLen >= 0 {
				return fmt.Errorf("size of column %s is not aligned", k)
			} else {
				d.valCols[ent.id] = vv
				lastLen = len(vv)
			}
		case []string:
			if ent, err := d.addColumn(k, true, false); err != nil {
				return err
			} else if len(vv) != lastLen && lastLen >= 0 {
				return fmt.Errorf("size of column %s is not aligned", k)
			} else {
				d.idCols[ent.id] = vv
				lastLen = len(vv)
			}
		default:
			return fmt.Errorf("invalid format of data are commited")
		}
	}

	d.shape[0] = lastLen
	d.shape[1] = len(data)
	return
}

func (d *DataFrame) registerColumns(types map[string]int) {
	d.reset()
	for k, v := range types {
		entry := ColEntry{
			Name: k,
		}

		switch v {
		case String:
			entry.id = int32(len(d.idCols))
			entry.tp = int8(v)
			d.idCols = append(d.idCols, nil)
		case Float32:
			entry.id = int32(len(d.valCols))
			entry.tp = int8(v)
			d.valCols = append(d.valCols, nil)
		default:
			continue
		}

		d.cols = append(d.cols, entry)
		d.colMap[k] = entry
	}

	d.shape[1] = len(d.cols)
}

func (d *DataFrame) addColumn(name string, id, alloc bool) (ret ColEntry, reterr error) {
	if _, suc := d.colMap[name]; suc {
		reterr = fmt.Errorf("column %s already exists in dataframe", name)
	}

	ret.Name = name
	if id {
		ret.tp = String
		ret.id = int32(len(d.idCols))
		if alloc {
			d.idCols = append(d.idCols, make([]string, d.shape[0]))
		} else {
			d.idCols = append(d.idCols, nil)
		}
	} else {
		ret.tp = Float32
		ret.id = int32(len(d.valCols))
		if alloc {
			d.valCols = append(d.valCols, make([]float32, d.shape[0]))
		} else {
			d.valCols = append(d.valCols, nil)
		}
	}

	d.cols = append(d.cols, ret)
	d.colMap[name] = ret
	d.shape[1] = len(d.cols)
	return
}

func (d *DataFrame) Shape() [2]int {
	return d.shape
}

func (d *DataFrame) Columns() (ret []string) {
	ret = make([]string, len(d.cols))
	for i, k := range d.cols {
		ret[i] = k.Name
	}

	return
}

func (d *DataFrame) haveColumns(cols []string) (ret bool, missed []string) {
	for _, col := range cols {
		if _, suc := d.colMap[col]; !suc {
			missed = append(missed, col)
		}
	}

	ret = len(missed) == 0
	return
}

func (d *DataFrame) getValueColumnNames() (ret []string) {
	for _, col := range d.cols {
		if col.tp == Float32 {
			ret = append(ret, col.Name)
		}
	}
	return
}

func (d *DataFrame) getIDColumnNames() (ret []string) {
	for _, col := range d.cols {
		if col.tp == String {
			ret = append(ret, col.Name)
		}
	}
	return
}

func (d *DataFrame) checkHeaders(cols []string) (reterr error) {
	_, missing, _ := CompareStrList(d.Columns(), cols)
	if len(missing) > 0 {
		reterr = fmt.Errorf("columns %v are not provided in the csv, provided columns = %v", missing, cols)
	}

	return
}

func (d *DataFrame) alloc(cnt int) {
	for i := range d.idCols {
		d.idCols[i] = make([]string, cnt)
	}

	for i := range d.valCols {
		d.valCols[i] = make([]float32, cnt)
	}

	d.shape[0] = cnt
}

func countLines(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func (d *DataFrame) smartColDet(f io.Reader) (reterr error) {
	cols := make(map[string]int)
	scanner := bufio.NewScanner(f)
	scanner.Scan()
	colNames := strings.Split(scanner.Text(), ",")
	colTypes := make([]int, len(colNames))
	for i := range colTypes {
		colTypes[i] = Float32
	}

	for i := 0; i < int(gSettings.ExploringDepth); i++ {
		if !scanner.Scan() {
			break
		}

		if err := scanner.Err(); err != nil {
			reterr = err
			return
		}

		eles := strings.Split(scanner.Text(), ",")
		for j := 0; j < len(eles); j++ {
			if colTypes[j] == Float32 {
				if _, err := strconv.ParseFloat(eles[j], 64); err != nil {
					colTypes[j] = String
				}
			}
		}
	}

	for i, tp := range colTypes {
		cols[colNames[i]] = tp
	}

	d.reset()
	d.registerColumns(cols)
	return
}

func (d *DataFrame) loadCSV(path string, smartCols bool) (reterr error) {
	f, reterr := os.Open(path)
	if reterr != nil {
		return
	}
	defer f.Close()

	if smartCols {
		d.smartColDet(f)
	}

	if _, reterr = f.Seek(0, 0); reterr != nil {
		return
	}

	lineCount, reterr := countLines(f)
	if reterr != nil {
		return
	}

	log.Printf("totally %d samples", lineCount)
	d.alloc(lineCount)

	if _, reterr = f.Seek(0, 0); reterr != nil {
		return
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, MaxCSVBufferSize), MaxCSVBufferSize)

	scanner.Scan()
	line := scanner.Text()
	cols := strings.Split(line, ",")
	if reterr = d.checkHeaders(cols); reterr != nil {
		return
	}

	var colEntries []ColEntry
	for _, col := range cols {
		colEntries = append(colEntries, d.colMap[col])
	}

	off := 0
	for scanner.Scan() {
		if reterr = scanner.Err(); reterr != nil {
			return
		}

		line := scanner.Text()
		eles := strings.Split(line, ",")
		if len(eles) == 0 {
			continue
		}

		for j, v := range eles {
			id := colEntries[j].id
			switch colEntries[j].tp {
			case String:
				d.idCols[id][off] = v
			case Float32:
				var val float64
				if val, reterr = strconv.ParseFloat(v, 32); reterr != nil {
					return
				}

				d.valCols[id][off] = float32(val)
			}
		}

		off++
	}

	return
}

func (d *DataFrame) getValCols(cols ...string) (ret [][]float32, reterr error) {
	colVals := make([][]float32, len(cols))
	for i, col := range cols {
		ent, suc := d.colMap[col]
		if !suc {
			return nil, fmt.Errorf("column %s does not exist", col)
		} else if ent.tp != Float32 {
			return nil, fmt.Errorf("column %s is not a value column", col)
		}

		colVals[i] = d.valCols[ent.id]
	}

	return colVals, nil
}

func (d *DataFrame) getIdCols(cols ...string) (ret [][]string, reterr error) {
	colVals := make([][]string, len(cols))
	for i, col := range cols {
		ent, suc := d.colMap[col]
		if !suc {
			return nil, fmt.Errorf("column %s does not exist", col)
		} else if ent.tp != String {
			return nil, fmt.Errorf("column %s is not a value column", col)
		}

		colVals[i] = d.idCols[ent.id]
	}

	return colVals, nil
}

func (d *DataFrame) SelectByColumns(cols ...string) (ret *DataFrame) {
	_, _, missing := CompareStrList(d.Columns(), cols)
	if len(missing) > 0 {
		panic(fmt.Errorf("required columns: %v are missing", missing))
		return
	}

	ret = &DataFrame{
		colMap: make(map[string]ColEntry),
	}

	for _, col := range cols {
		ent := d.colMap[col]

		switch ent.tp {
		case String:
			ent.id = int32(len(ret.idCols))
			ret.idCols = append(ret.idCols, d.idCols[ent.id])
		case Float32:
			ent.id = int32(len(ret.valCols))
			ret.valCols = append(ret.valCols, d.valCols[ent.id])
		}

		ret.colMap[col] = ent
		ret.cols = append(ret.cols, ent)
	}

	ret.shape[0] = d.shape[0]
	ret.shape[1] = len(cols)
	return
}

func (d *DataFrame) SelectByRows(rows ...int32) (ret *DataFrame) {
	for _, id := range rows {
		if id >= int32(d.shape[0]) {
			panic(fmt.Errorf("id %d exceeds the dataframe size", id))
			return
		}
	}

	ret = &DataFrame{
		colMap:  make(map[string]ColEntry),
		cols:    make([]ColEntry, len(d.cols)),
		idCols:  make([][]string, len(d.idCols)),
		valCols: make([][]float32, len(d.valCols)),
		shape:   [2]int{len(rows), d.shape[1]},
	}

	copy(ret.cols, d.cols)
	for k, v := range d.colMap {
		ret.colMap[k] = v
	}

	for i := range ret.idCols {
		ret.idCols[i] = sliceSelectString(d.idCols[i], rows)
	}

	for i := range ret.valCols {
		ret.valCols[i] = sliceSelectFloat32(d.valCols[i], rows)
	}

	return
}

func (d *DataFrame) SelectRange(beg, end int32) (ret *DataFrame, reterr error) {
	if beg < 0 {
		reterr = fmt.Errorf("param beg is less than 0")
		return
	}

	if end > int32(d.shape[0]) {
		reterr = fmt.Errorf("param end exceeds maxiumn size of data frame")
		return
	}

	ret = &DataFrame{
		colMap:  make(map[string]ColEntry),
		cols:    make([]ColEntry, len(d.cols)),
		idCols:  make([][]string, len(d.cols)),
		valCols: make([][]float32, len(d.cols)),
		shape:   [2]int{int(end - beg), d.shape[1]},
	}

	copy(ret.cols, d.cols)
	for k, v := range d.colMap {
		ret.colMap[k] = v
	}

	for i := range ret.idCols {
		ret.idCols[i] = d.idCols[i][beg:end]
	}

	for i := range ret.valCols {
		ret.valCols[i] = d.valCols[i][beg:end]
	}

	return
}

func (d *DataFrame) MinValues(cols ...string) (ret []float32, reterr error) {
	ret = make([]float32, len(cols))
	for i, col := range cols {
		if ent, suc := d.colMap[col]; !suc {
			reterr = fmt.Errorf("column %s is not in the dataframe", col)
			return
		} else if tp := ent.tp; tp == String {
			reterr = fmt.Errorf("min value operation cannot been applied to non-value columns")
			return
		} else {
			ret[i] = minOfSlice(d.valCols[ent.id])
		}
	}

	return
}

func (d *DataFrame) MaxValues(cols ...string) (ret []float32, reterr error) {
	ret = make([]float32, len(cols))
	for i, col := range cols {
		if ent, suc := d.colMap[col]; !suc {
			reterr = fmt.Errorf("column %s is not in the dataframe", col)
			return
		} else if tp := ent.tp; tp == String {
			reterr = fmt.Errorf("min value operation cannot been applied to non-value columns")
			return
		} else {
			ret[i] = maxOfSlice(d.valCols[ent.id])
		}
	}

	return
}

func (d *DataFrame) GroupBy(cols ...string) (ret *DataFrameWithGroupBy) {
	ret = &DataFrameWithGroupBy{}
	if err := ret.buildFromDF(d, cols); err != nil {
		ret = nil
		log.Printf("Error: %v", err)
	}
	return
}

func (d *DataFrame) applyOnColumn(col []float32, op interface{}) (ret float32) {
	switch p := op.(type) {
	case BinaryOP:
		base := p.InitValue()
		for _, v := range col {
			base = p.Operate(base, v)
		}
		ret = base
	case ListOP:
		ret = p(col)
	}

	return
}

func (d *DataFrame) Apply(ops map[string]interface{}) (ret map[string]float32) {
	opList := flatternOpList(ops)
	res := make([]float32, len(opList))
	reqCols := make([]string, len(opList))
	colVals := make([][]float32, len(opList))
	for i, op := range opList {
		if !validOperation(op.Op) {
			panic(fmt.Errorf("invalid type of operation for column: %s", op.col))
		}

		if ent, suc := d.colMap[op.col]; !suc {
			panic(fmt.Errorf("Column %s does not exist in the dataframe", op.col))
		} else if ent.tp != Float32 {
			panic(fmt.Errorf("Column %s is of type ID, which does not support apply in dataframe", op.col))
		} else {
			reqCols[i] = op.col
			colVals[i] = d.valCols[d.colMap[op.col].id]
		}
	}

	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < len(opList); i += int(gSettings.ThreadNum) {
			res[i] = d.applyOnColumn(colVals[i], opList[i].Op)
		}
	})

	ret = make(map[string]float32)
	for i, p := range opList {
		ret[p.col+p.Surfix] = res[i]
	}

	return
}

func (d *DataFrame) Empty() bool {
	return len(d.cols) == 0
}

func (d *DataFrame) LeftMerge(t *DataFrame, on []string, tcols []string, suffix string, firstMatchOnly bool) (ret *DataFrame) {
	gd := d.GroupBy(on...)
	if len(tcols) > 0 {
		t = t.SelectByColumns(append(on, tcols...)...)
	}
	gt := t.GroupBy(on...)

	onSet := make(map[string]struct{})
	for _, v := range on {
		onSet[v] = struct{}{}
	}

	return gd.LeftMerge(gt, suffix, firstMatchOnly)
}

func ReadCSVWithHeaderInfo(header map[string]int, path string) (ret *DataFrame, reterr error) {
	defer func() {
		if reterr != nil {
			ret = nil
		}
	}()

	ret = &DataFrame{}
	ret.registerColumns(header)
	reterr = ret.loadCSV(path, false)
	return
}

func ReadCSV(path string) (ret *DataFrame, reterr error) {
	defer func() {
		if reterr != nil {
			ret = nil
		}
	}()

	ret = &DataFrame{}
	reterr = ret.loadCSV(path, true)
	return
}

func CreateByData(data map[string]interface{}) (ret *DataFrame, reterr error) {
	ret = &DataFrame{}
	if reterr = ret.createWithData(data); reterr != nil {
		ret = nil
	}
	return
}

func (d *DataFrame) Copy(copyData bool) (ret *DataFrame) {
	ret = &DataFrame{}
	ret.colMap = make(map[string]ColEntry)
	for k, v := range d.colMap {
		ret.colMap[k] = v
	}

	ret.cols = make([]ColEntry, len(d.cols))
	copy(ret.cols, d.cols)

	ret.idCols = make([][]string, len(d.idCols))
	ret.valCols = make([][]float32, len(d.valCols))
	ret.shape = d.shape

	if copyData {
		ret.alloc(ret.shape[0])

		for i := range d.idCols {
			copy(ret.idCols[i], d.idCols[i])
		}

		for i := range d.valCols {
			copy(ret.valCols[i], d.valCols[i])
		}
	} else {
		ret.shape = d.shape
		for i := range d.idCols {
			ret.idCols[i] = d.idCols[i]
		}

		for i := range d.valCols {
			ret.valCols[i] = d.valCols[i]
		}
	}

	return
}
