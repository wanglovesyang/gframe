package gframe

import "testing"

func TestDataFrameShow_full(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"D": []float32{1, 2, 3, 4, 5, 6, 7},
		"E": []float32{1, 2, 3, 4, 5, 6, 7},
		"F": []float32{1, 2, 3, 4, 5, 6, 7},
		"G": []float32{1, 2, 3, 4, 5, 6, 7},
		"H": []float32{1, 2, 3, 4, 5, 6, 7},
		"I": []float32{1, 2, 3, 4, 5, 6, 7},
	})

	d.Show()
}
