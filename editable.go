package gframe

import "fmt"

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

func (e *EditableDataFrame) GetIdColumns(cols []string) (ret [][]string, reterr error) {
	return e.getIdCols(cols)
}

func (e *EditableDataFrame) GetValColumns(cols []string) (ret [][]float32, reterr error) {
	return e.getValCols(cols)
}

func (e *EditableDataFrame) PasteIdColumn(col string, ids []string) (reterr error) {
	if len(ids) != e.shape[0] {
		return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", int32(len(ids)), e.shape[0])
	}

	ent, suc := e.colMap[col]
	if !suc {
		return fmt.Errorf("column %s does not exist in dataframe", col)
	}

	if ent.tp != String {
		reterr = fmt.Errorf("column %s is of type ID", col)
		return
	}

	e.idCols[ent.id] = ids
	return
}

func (e *EditableDataFrame) PasteValColumn(col string, vals []float32) (reterr error) {
	if len(vals) != e.shape[0] {
		return fmt.Errorf("the size of provided column are not consistent with dataframe [%d / %d]", len(vals), e.shape[0])
	}

	ent, suc := e.colMap[col]
	if !suc {
		return fmt.Errorf("column %s does not exist in dataframe", col)
	}

	if ent.tp != Float32 {
		reterr = fmt.Errorf("column %s is of type Value", col)
		return
	}

	e.valCols[ent.id] = vals
	return
}

func (e *EditableDataFrame) Calculate(newCol string, fc interface{}) (reterr error) {
	return
}
