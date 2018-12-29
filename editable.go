package gframe

import (
	"fmt"
	"log"
	"reflect"
)

type EditableDataFrame struct {
	DataFrame
}

func (e *EditableDataFrame) SetId(rowId int32, col string, id string) (reterr error) {
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

func (e *EditableDataFrame) SetValue(rowId int32, col string, val float32) (reterr error) {
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

func (e *EditableDataFrame) GetIdColumns(cols ...string) (ret [][]string, reterr error) {
	return e.getIdCols(cols...)
}

func (e *EditableDataFrame) GetValColumns(cols ...string) (ret [][]float32, reterr error) {
	return e.getValCols(cols...)
}

func (e *EditableDataFrame) PasteIdColumn(col string, ids []string) (reterr error) {
	if len(ids) != e.shape[0] {
		return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", int32(len(ids)), e.shape[0])
	}

	ent, suc := e.colMap[col]
	if !suc {
		ent = e.addColumn(col, true, false)
	}

	if ent.tp != String {
		reterr = fmt.Errorf("column %s is of type ID", col)
		return
	}

	e.idCols[ent.id] = ids
	return
}

func (e *EditableDataFrame) PasteValColumn(col string, vals []float32) (reterr error) {
	if !e.Empty() {
		if len(vals) != e.shape[0] {
			return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", len(vals), e.shape[0])
		}
	} else {
		e.shape[0] = len(vals)
	}

	ent, suc := e.colMap[col]
	if !suc {
		ent = e.addColumn(col, false, false)
	}

	if ent.tp != Float32 {
		reterr = fmt.Errorf("column %s is of type Value", col)
		return
	}

	e.valCols[ent.id] = vals
	e.shape[1] = len(e.cols)
	return
}

func (e *EditableDataFrame) makeArgList(cols []ColEntry, rowId int32) (ret []reflect.Value) {
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

func (e *EditableDataFrame) fillCalculateResult(cols []ColEntry, res []reflect.Value, rowId int32) {
	for i, col := range cols {
		if col.tp == String {
			e.idCols[col.id][rowId] = res[i].String()
		} else if col.tp == Float32 {
			e.valCols[col.id][rowId] = float32(res[i].Float())
		}
	}
}

func (e *EditableDataFrame) Calculate(argCols, retCols []string, fc interface{}) (reterr error) {
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
			ent := e.addColumn(col.Name, col.tp == String, true)
			retColsE[i] = ent
		}
	}

	fcv := reflect.ValueOf(fc)
	Parallel(int(gSettings.ThreadNum), func(id int) {
		for i := id; i < e.shape[0]; i += int(gSettings.ThreadNum) {
			vals := e.makeArgList(argColsE, int32(i))
			res := fcv.Call(vals)
			if withErr {
				lastErr := vals[len(vals)-1].Interface()
				log.Printf("Error in calculating, %v", lastErr)
				reterr = lastErr.(error)
			}

			e.fillCalculateResult(retColsE, res, int32(i))
		}
	})

	return
}

func Empty() (ret *EditableDataFrame) {
	ret = &EditableDataFrame{}
	ret.reset()
	return
}

func (e *EditableDataFrame) Concatenate(f *DataFrame, vertical bool) (reterr error) {
	if vertical {
		return e.concatenateVertical(f)
	}

	return e.concatenateHorizontal(f)
}

func (e *EditableDataFrame) concatenateVertical(f *DataFrame) (reterr error) {
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

func (e *EditableDataFrame) concatenateHorizontal(f *DataFrame) (reterr error) {
	if f.shape[0] != e.shape[0] {
		return fmt.Errorf("cannot concatenate horizontally when two data frame are ")
	}

	for _, col := range f.cols {
		if col.tp == String {
			ss := copyStringSlice(f.idCols[col.id])
			e.PasteIdColumn(col.Name, ss)
		} else if col.tp == Float32 {
			ss := copyFloat32Slice(f.valCols[col.id])
			e.PasteValColumn(col.Name, ss)
		}
	}

	return
}
