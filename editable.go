package gframe

import (
	"fmt"
	"reflect"
	"time"
)

func (e *DataFrame) DropColumnInplace(col string) (reterr error) {
	ent, suc := e.colMap[col]
	if !suc {
		reterr = fmt.Errorf("column %s does not exist in dataframe", col)
		return
	}

	var offInCols int
	for i, col := range e.cols {
		if col.tp == ent.tp {
			if col.id > ent.id {
				e.cols[i].id--
			} else if col.id == ent.id {
				offInCols = i
			}
		}
	}

	if offInCols < len(e.cols)-1 {
		copy(e.cols[offInCols:len(e.cols)-1], e.cols[offInCols+1:])
	}
	e.cols = e.cols[0 : len(e.cols)-1]

	if ent.tp == String {
		if ent.id != int32(len(e.idCols)-1) {
			// non last columns
			copy(e.idCols[ent.id:int32(len(e.idCols)-1)], e.idCols[ent.id+1:])
		}
		e.idCols = e.idCols[0 : len(e.idCols)-1]
	} else if ent.tp == Float32 {
		if ent.id != int32(len(e.valCols)-1) {
			// non last columns
			copy(e.valCols[ent.id:int32(len(e.valCols)-1)], e.valCols[ent.id+1:])
		}
		e.valCols = e.valCols[0 : len(e.valCols)-1]
	}

	return
}

func (e *DataFrame) SetId(rowId int32, col string, id string) (reterr error) {
	ent, suc := e.colMap[col]
	if !suc {
		reterr = fmt.Errorf("column %s does not exist in dataframe", col)
		return
	}

	if ent.tp != String {
		reterr = fmt.Errorf("column %s is of type ID", col)
		return
	}

	if rowId >= int32(e.shape[0]) {
		reterr = fmt.Errorf("row id [%d] exceeds data frame", rowId)
		return
	}

	e.idCols[ent.id][rowId] = id
	return
}

func (e *DataFrame) SetValue(rowId int32, col string, val float32) (reterr error) {
	ent, suc := e.colMap[col]
	if !suc {
		reterr = fmt.Errorf("column %s does not exist in dataframe", col)
		return
	}

	if ent.tp != String {
		reterr = fmt.Errorf("column %s is of type Value", col)
		return
	}

	if rowId >= int32(e.shape[0]) {
		reterr = fmt.Errorf("row id [%d] exceeds data frame", rowId)
		return
	}

	e.valCols[ent.id][rowId] = val
	return
}

func (e *DataFrame) GetIdColumns(cols ...string) (ret [][]string, reterr error) {
	return e.getIdCols(cols...)
}

func (e *DataFrame) GetValColumns(cols ...string) (ret [][]float32, reterr error) {
	return e.getValCols(cols...)
}

func (e *DataFrame) PasteIdColumn(col string, ids []string) (reterr error) {
	if len(ids) != e.shape[0] {
		return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", int32(len(ids)), e.shape[0])
	}

	ent, suc := e.colMap[col]
	if !suc {
		if ent, reterr = e.addColumn(col, true, false); reterr != nil {
			return
		}
	}

	if ent.tp != String {
		reterr = fmt.Errorf("column %s is of type ID", col)
		return
	}

	e.idCols[ent.id] = ids
	return
}

func (e *DataFrame) PasteValColumn(col string, vals []float32) (reterr error) {
	if !e.Empty() {
		if len(vals) != e.shape[0] {
			return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", len(vals), e.shape[0])
		}
	} else {
		e.shape[0] = len(vals)
	}

	ent, suc := e.colMap[col]
	if !suc {
		if ent, reterr = e.addColumn(col, false, false); reterr != nil {
			return
		}
	}

	if ent.tp != Float32 {
		reterr = fmt.Errorf("column %s is of type Value", col)
		return
	}

	e.valCols[ent.id] = vals
	e.shape[1] = len(e.cols)
	return
}

func (e *DataFrame) makeArgList(cols []ColEntry, rowId int32) (ret []reflect.Value) {
	ret = make([]reflect.Value, len(cols))
	for i, col := range cols {
		if col.tp == String {
			ret[i] = reflect.ValueOf(e.idCols[col.id][rowId])
		} else if col.tp == Float32 {
			ret[i] = reflect.ValueOf(e.valCols[col.id][rowId])
		}
	}

	return
}

func (e *DataFrame) fillCalculateResult(cols []ColEntry, res []reflect.Value, rowId int32) {
	for i, col := range cols {
		if col.tp == String {
			e.idCols[col.id][rowId] = res[i].String()
		} else if col.tp == Float32 {
			e.valCols[col.id][rowId] = float32(res[i].Float())
		}
	}
}

func (e *DataFrame) Calculate(argCols, retCols []string, fc interface{}) (reterr error) {
	if gSettings.Profiling {
		tStart := time.Now()
		defer func() {
			tEnd := time.Now()
			Log("Cost of column caculate: %fms", tEnd.Sub(tStart).Seconds()*1000)
		}()
	}

	argColsE := make([]ColEntry, len(argCols))
	for i, col := range argCols {
		if ent, suc := e.colMap[col]; !suc {
			reterr = fmt.Errorf("column %s is not in the dataframe", col)
			return
		} else {
			argColsE[i] = ent
		}
	}

	retColsE := make([]ColEntry, len(retCols))
	for i, col := range retCols {
		if ent, suc := e.colMap[col]; !suc {
			retColsE[i] = ColEntry{
				Name: col,
				id:   Unknown,
				tp:   Unknown,
			}
		} else {
			retColsE[i] = ent
		}
	}

	if reterr = checkParamterType(fc, argColsE); reterr != nil {
		return
	}

	var withErr bool
	if withErr, reterr = checkReturnType(fc, retColsE); reterr != nil {
		return
	}

	for i, col := range retColsE {
		if col.id < 0 {
			if ent, err := e.addColumn(col.Name, col.tp == String, true); err != nil {
				return err
			} else {
				retColsE[i] = ent
			}
		}
	}

	fcv := reflect.ValueOf(fc)
	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < e.shape[0]; i += int(gSettings.ThreadNum) {
			vals := e.makeArgList(argColsE, int32(i))
			res := fcv.Call(vals)
			if withErr {
				lastErr := vals[len(vals)-1].Interface()
				Log("Error in calculating, %v", lastErr)
				reterr = lastErr.(error)
			}

			e.fillCalculateResult(retColsE, res, int32(i))
		}
	})

	return
}

func Empty() (ret *DataFrame) {
	ret = &DataFrame{}
	ret.reset()
	return
}

func (e *DataFrame) Concatenate(f *DataFrame, vertical bool) (ret *DataFrame) {
	var reterr error
	defer func() {
		if reterr != nil {
			panic(reterr)
		}
	}()

	ret = e.Copy(false)
	reterr = ret.ConcatenateInplace(f, vertical)
	return
}

func (e *DataFrame) ConcatenateInplace(f *DataFrame, vertical bool) (reterr error) {
	if vertical {
		return e.concatenateVertical(f)
	}

	return e.concatenateHorizontal(f)
}

func (e *DataFrame) concatenateVertical(f *DataFrame) (reterr error) {
	if !compareColumns(e.colMap, f.colMap) {
		return fmt.Errorf("Cannot concatenate vertically when two dataframes are with different columns")
	}

	for _, col := range e.cols {
		sid := col.id
		tid := f.colMap[col.Name].id

		if col.tp == String {
			e.idCols[sid] = append(e.idCols[sid], f.idCols[tid]...)
		} else {
			e.valCols[sid] = append(e.valCols[sid], f.valCols[tid]...)
		}
	}

	e.shape[0] += f.shape[0]
	return
}

func (e *DataFrame) concatenateHorizontal(f *DataFrame) (reterr error) {
	if f.shape[0] != e.shape[0] {
		return fmt.Errorf("cannot concatenate horizontally when two data frame are ")
	}

	for _, col := range f.cols {
		if col.tp == String {
			ss := copyStringSlice(f.idCols[col.id])
			if reterr = e.PasteIdColumn(col.Name, ss); reterr != nil {
				return
			}
		} else if col.tp == Float32 {
			ss := copyFloat32Slice(f.valCols[col.id])
			if reterr = e.PasteValColumn(col.Name, ss); reterr != nil {
				return
			}
		}
	}

	return
}
