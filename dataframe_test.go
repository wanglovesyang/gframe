package gframe

import (
	"sync"
	"testing"
)

func TestDataFrame_registerColumns(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"c": -1,
			"d": -1,
			"e": Float32,
		},
	)

	if d.shape[1] != 3 {
		t.Fatalf("incorrect columns dimension when pass extra invalid dimenstions")
	}

	for _, col := range []string{"a", "b", "e"} {
		if _, suc := d.colMap[col]; !suc {
			t.Errorf("Column %s is not in the dataframe", col)
		}
	}
}

func TestDataFrame_addColumn_if_duplicate_then_panic(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	func() {
		defer func() {
			if err := recover(); err == nil {
				t.Fatalf("adding duplicate column does not case any error panic")
			}
		}()
		d.addColumn("a", true, true)
	}()
}

func TestDataFrame_addColumn_consistency_check(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	d.addColumn("d", true, true)
	if ent, suc := d.colMap["d"]; !suc {
		t.Fatalf("column d is missing after addcolumn is called")
	} else if ent.tp != String {
		t.Errorf("column d is not of string type")
	} else if d.idCols[ent.id] == nil {
		t.Errorf("columns d is not allocated")
	}

	d.addColumn("c", false, false)
	if ent, suc := d.colMap["c"]; !suc {
		t.Fatalf("column c is missing after addcolumn is called")
	} else if ent.tp != Float32 {
		t.Errorf("column c is not of float type")
	} else if d.valCols[ent.id] != nil {
		t.Errorf("columns d is allocated")
	}
}

func TestDataFrame_Columns_consistenty(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	cols := d.Columns()
	if len(cols) != len(d.cols) {
		t.Fatalf("inconsistent length of columns return")
	}

	for i, c := range cols {
		if c != d.cols[i].Name {
			t.Errorf("inconsisent columns names [%s / %s]", c, d.cols[i].Name)
		}
	}
}

func TestDataFrame_checkHeaders_missing_columns(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	if reterr := d.checkHeaders([]string{"G"}); reterr == nil {
		t.Errorf("error does not shown up when invalid column is provided")
	}
}

func TestDataFrame_alloc(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	d.alloc(10)

	if d.shape[0] != 10 {
		t.Errorf("invalid shape var")
	}

	for _, col := range d.cols {
		if col.tp == String {
			if len(d.idCols[col.id]) != 10 {
				t.Errorf("inconsistent column[%s] arr len", col.Name)
			}
		} else if col.tp == Float32 {
			if len(d.valCols[col.id]) != 10 {
				t.Errorf("inconsistent column[%s] arr len", col.Name)
			}
		}
	}
}

func TestDataFrame_LoadCSV_error_check(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	if err := d.loadCSV("does/not/exist", false); err == nil {
		t.Errorf("nil error when loading a non-exist csv")
	}

	if err := d.loadCSV("illpose.csv", false); err == nil {
		t.Errorf("nil error when loading a ill-posed csv")
	}
}

func TestDataFrame_LoadCSV_consistency_check(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	if err := d.loadCSV("correct.csv", false); err != nil {
		t.Fatalf("loading err: %v", err)
	}

	for _, col := range d.cols {
		if col.Name == "a" {
			if d.idCols[col.id][0] != "a" {
				t.Errorf("invalid columns value for a")
			}
		} else if col.Name == "b" {
			if int(d.valCols[col.id][0]) != 12 {
				t.Errorf("invalid columns value for b")
			}
		} else if col.Name == "e" {
			if int(d.valCols[col.id][0]) != 13 {
				t.Errorf("invalid columns value for e")
			}
		}
	}
}

func TestData_getValCols(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	if err := d.loadCSV("correct.csv", false); err != nil {
		t.Fatalf("loading err: %v", err)
	}

	res, err := d.getValCols("b")
	if err != nil {
		t.Fatalf("error returned in getValCols %v", err)
	}

	if len(res) != 1 {
		t.Errorf("invalid return size")
	}

	if int(res[0][0]) != 12 {
		t.Errorf("inconsistent value")
	}

	if _, err = d.getValCols("a"); err == nil {
		t.Errorf("err is nil when passing idcolumns")
	}
}

func TestData_getIdCols(t *testing.T) {
	d := DataFrame{}
	d.registerColumns(
		map[string]int{
			"a": String,
			"b": Float32,
			"e": Float32,
		},
	)

	if err := d.loadCSV("correct.csv", false); err != nil {
		t.Fatalf("loading err: %v", err)
	}

	res, err := d.getIdCols("a")
	if err != nil {
		t.Fatalf("error returned in getValCols %v", err)
	}

	if len(res) != 1 {
		t.Errorf("invalid return size")
	}

	if res[0][0] != "a" {
		t.Errorf("inconsistent value")
	}

	if _, err = d.getIdCols("e"); err == nil {
		t.Errorf("err is nil when value valcolumns")
	}
}

func TestDataFrame_SelectByRows(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	dd := d.SelectByRows(0)
	if dd.shape[0] != 1 {
		t.Errorf("invalid height of selected")
	}

	if dd.shape[1] != d.shape[1] {
		t.Errorf("invalid width of selected")
	}

	if dd.idCols[0][0] != "1" {
		t.Errorf("incorrect selected data")
	}
}

func TestDataFrame_SelectByColumns(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	dd := d.SelectByColumns("b")
	if dd.shape[0] != d.shape[0] {
		t.Errorf("invalid height of selected")
	}

	if dd.shape[1] != 1 {
		t.Errorf("invalid width of selected")
	}

	if int(dd.valCols[0][1]) != 2 {
		t.Errorf("incorrect selected data [%f, 2]", dd.valCols[0][1])
	}
}

func TestDataFrame_SelectRange(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	dd := d.SelectByRows(0, 1)
	if dd.shape[0] != 2 {
		t.Errorf("invalid height of selected")
	}

	if dd.shape[1] != d.shape[1] {
		t.Errorf("invalid width of selected")
	}

	if dd.idCols[0][1] != "2" {
		t.Errorf("incorrect selected data")
	}
}

func TestDataFrame_MaxValues_error_check(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	if _, err := d.MaxValues("d"); err == nil {
		t.Errorf("nil error when column is not in dataframe")
	}

	if _, err := d.MaxValues("a"); err == nil {
		t.Error("nill error when id column is given")
	}
}

func TestDataFrame_MaxValues_check(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	res, err := d.MaxValues("b", "c")
	if err != nil {
		t.Fatalf("error %v", err)
	}

	if int(res[0]) != 3 {
		t.Errorf("max value for b is incorrect")
	}

	if res[1] != 0.3 {
		t.Errorf("max value for c is incorrect")
	}
}

func TestDataFrame_MinValues_check(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})

	res, err := d.MinValues("b", "c")
	if err != nil {
		t.Fatalf("error %v", err)
	}

	if int(res[0]) != 1 {
		t.Errorf("max value for b is incorrect")
	}

	if res[1] != 0.1 {
		t.Errorf("max value for c is incorrect")
	}
}

var dfForAplly *DataFrame
var onceInitForApply sync.Once

func initApplyDf() {
	dfForAplly = &DataFrame{}
	dfForAplly.createWithData(map[string]interface{}{
		"a": []string{"1", "2", "3"},
		"b": []float32{1, 2, 3},
		"c": []float32{0.1, 0.2, 0.3},
	})
}

func TestDataFrame_Apply_error_if_valid_op_then_error(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("No panic when passing invalid operation")
		}
	}()

	onceInitForApply.Do(initApplyDf)
	dfForAplly.Apply(map[string]interface{}{
		"b": func(a, b, c float32) {},
	})
}

func TestDataFrame_Apply_error_if_missing_col_then_error(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("No panic when passing non-exist columns")
		}
	}()

	onceInitForApply.Do(initApplyDf)
	dfForAplly.Apply(map[string]interface{}{
		"j": ReduceMax,
	})
}

func TestDataFrame_Apply_error_if_id_col_then_error(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("No panic when passing id columns")
		}
	}()

	onceInitForApply.Do(initApplyDf)
	dfForAplly.Apply(map[string]interface{}{
		"a": ReduceMax,
	})
}

func TestDataFrame_Apply_consistency_check(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Errorf("error panics, %v", err)
		}
	}()

	res := dfForAplly.Apply(map[string]interface{}{
		"b": ReduceMax,
		"c": ReduceMin,
	})

	if int(res["b"]) != 3 {
		t.Errorf("invalid apply result for column b, %v", res["b"])
	}

	if res["c"] != 0.1 {
		t.Errorf("invalid apply result for column c, %v", res["c"])
	}
}

func TestDataFrame_LeftMerge(t *testing.T) {

}

func TestDataFrame_smartColDet(t *testing.T) {
	d := &DataFrame{}
	if err := d.loadCSV("correct.csv", true); err != nil {
		t.Errorf("Error on loading, %v", err)
	}

	if d.colMap["a"].tp != String {
		t.Errorf("Invalid data type of column a")
	}

	if d.colMap["b"].tp != Float32 {
		t.Errorf("Invalid data type of column b")
	}

	if d.colMap["e"].tp != Float32 {
		t.Errorf("Invalid data type of column e")
	}
}
