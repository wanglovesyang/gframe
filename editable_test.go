package gframe

import (
	"testing"
)

func TestCalculate_if_source_missing_columns_then_error(t *testing.T) {
	df := &DataFrame{}
	df.createWithData(
		map[string]interface{}{
			"a": []string{"g1", "g2", "g3", "g1", "g2", "g3", "g1", "g3"},
			"b": []float32{1, 2, 3, 4, 5, 6, 7, 8},
			"c": []float32{15, 16, 17, 18, 19, 20, 21, 22},
		},
	)

	if err := df.Calculate([]string{"j"}, []string{"s"}, nil); err == nil {
		t.Errorf("nil error upon magic column input")
	}
}

func TestCalculate_if_function_type_incorrect_then_error(t *testing.T) {
	df := &DataFrame{}
	df.createWithData(
		map[string]interface{}{
			"a": []string{"g1", "g2", "g3", "g1", "g2", "g3", "g1", "g3"},
			"b": []float32{1, 2, 3, 4, 5, 6, 7, 8},
			"c": []float32{15, 16, 17, 18, 19, 20, 21, 22},
		},
	)

	f1 := func(b, c float32) float32 {
		Log("%f + %f = %f", b, c, b+c)
		return b + c
	}

	if err := df.Calculate([]string{"a", "b"}, []string{"c"}, f1); err == nil {
		t.Errorf("nil error when function argument are disaligned with columns")
	}

	if err := df.Calculate([]string{"c", "b"}, []string{"a"}, f1); err == nil {
		t.Errorf("nil error when function ouput are disaligned with columns")
	}
}

func TestCalculate_consistency(t *testing.T) {
	df := &DataFrame{}
	df.createWithData(
		map[string]interface{}{
			"a": []string{"g1", "g2", "g3", "g1", "g2", "g3", "g1", "g3"},
			"b": []float32{1, 2, 3, 4, 5, 6, 7, 8},
			"c": []float32{15, 16, 17, 18, 19, 20, 21, 22},
		},
	)

	f1 := func(b, c float32) float32 {
		//Log("%f + %f = %f", b, c, b+c)
		return b + c
	}

	if err := df.Calculate([]string{"c", "b"}, []string{"d"}, f1); err != nil {
		t.Errorf("Error in calculating, %v", err)
	}

	groundTruth := []float32{
		16, 18, 20, 22, 24, 26, 28, 30,
	}

	if df.shape[1] != 4 {
		t.Errorf("invalid shape after calculation")
	}

	d, err := df.GetValColumns("d")
	if err != nil {
		t.Errorf("new column does not show up")
	}

	dd := d[0]
	for i, dv := range dd {
		if dv != groundTruth[i] {
			t.Errorf("calculation res does not align with ground truth, [%f, %f]", dv, groundTruth[i])
		}
	}
}
