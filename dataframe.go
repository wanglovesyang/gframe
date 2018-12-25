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
	String = iota
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
	columnNames []string
	cols        []ColEntry
	colMap      map[string]ColEntry

	shape   [2]int
	idCols  [][]string
	valCols [][]float32
}

func (d *DataFrame) registerColumns(types map[string]int) {
	d.colMap = make(map[string]ColEntry)
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
		}

		d.cols = append(d.cols, entry)
		d.colMap[k] = entry

		d.columnNames = append(d.columnNames, k)
	}

	d.shape[1] = len(d.columnNames)
}

func (d *DataFrame) Shape() [2]int {
	return d.shape
}

func (d *DataFrame) Columns() []string {
	return d.columnNames
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
	if len(cols) != len(d.colMap) {
		return fmt.Errorf("inconsistent csv header with regiestered")
	}

	for _, col := range cols {
		if _, suc := d.colMap[col]; !suc {
			return fmt.Errorf("Columns %s does not appear in registered list", col)
		}
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

func (d *DataFrame) LoadCSV(path string) (reterr error) {
	f, reterr := os.Open(path)
	if reterr != nil {
		return
	}
	defer f.Close()

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

func (d *DataFrame) getValCols(cols []string) (ret [][]float32, reterr error) {
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

func (d *DataFrame) getIdCols(cols []string) (ret [][]string, reterr error) {
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

func (d *DataFrame) SelectByColumns(cols ...string) (ret *DataFrame, reterr error) {
	_, _, missing := CompareStrList(d.columnNames, cols)
	if len(missing) > 0 {
		reterr = fmt.Errorf("required columns: %v are missing", missing)
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
		ret.columnNames = append(ret.columnNames, col)
	}

	ret.shape[0] = d.shape[0]
	ret.shape[1] = len(cols)
	return
}

func (d *DataFrame) SelectByRows(rows []int32) (ret *DataFrame, reterr error) {
	for _, id := range rows {
		if id >= int32(d.shape[0]) {
			reterr = fmt.Errorf("id %d exceeds the dataframe size", id)
			return
		}
	}

	ret = &DataFrame{
		colMap:      make(map[string]ColEntry),
		cols:        make([]ColEntry, len(d.cols)),
		columnNames: make([]string, len(d.cols)),
		idCols:      make([][]string, len(d.cols)),
		valCols:     make([][]float32, len(d.cols)),
		shape:       [2]int{len(rows), d.shape[1]},
	}

	copy(ret.cols, d.cols)
	copy(ret.columnNames, d.columnNames)
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
		colMap:      make(map[string]ColEntry),
		cols:        make([]ColEntry, len(d.cols)),
		columnNames: make([]string, len(d.cols)),
		idCols:      make([][]string, len(d.cols)),
		valCols:     make([][]float32, len(d.cols)),
		shape:       [2]int{int(end - beg), d.shape[1]},
	}

	copy(ret.cols, d.cols)
	copy(ret.columnNames, d.columnNames)
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
		ent := d.colMap[col]
		if tp := ent.tp; tp == String {
			reterr = fmt.Errorf("min value operation cannot been applied to non-value columns")
			return
		}

		ret[i] = minOfSlice(d.valCols[ent.id])
	}

	return
}

func (d *DataFrame) MaxValues(cols ...string) (ret []float32, reterr error) {
	ret = make([]float32, len(cols))
	for i, col := range cols {
		ent := d.colMap[col]
		if tp := ent.tp; tp == String {
			reterr = fmt.Errorf("min value operation cannot been applied to non-value columns")
			return
		}

		ret[i] = maxOfSlice(d.valCols[ent.id])
	}

	return
}

func (d *DataFrame) getKeyIDTuples(keyCols []string) (ret []KeyID, reterr error) {
	kc, reterr := d.getIdCols(keyCols)
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

func (d *DataFrame) GroupBy(cols ...string) (ret *DataFrameWithGroupBy) {
	ret = &DataFrameWithGroupBy{}
	if err := ret.buildFromDF(d, cols); err != nil {
		ret = nil
		log.Printf("Error: %v", err)
	}
	return
}

func (d *DataFrame) Edit() (ret *EditableDataFrame) {
	return &EditableDataFrame{DataFrame: *d}
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

func (d *DataFrame) LeftMerge(on []string) (ret *DataFrame) {
	return
}

func (d *DataFrame) Show() {
	//TODO: show
}
