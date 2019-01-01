package gframe

import "testing"

func TestDataFrameShow_full(t *testing.T) {
	d := DataFrame{}
	d.createWithData(map[string]interface{}{
		"S":    []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"SS":   []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"SSS":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"SSSS": []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"KKK":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"OOO":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"GGG":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"JJJ":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"HHH":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"QQQ":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"PPP":  []string{"aaaa", "bbbb", "cccc", "dddd", "eeee", "a", "b"},
		"D":    []float32{1, 2, 3, 4, 5, 6, 7},
		"E":    []float32{11, 2222, 3, 414, 5, 6, 7},
		"F":    []float32{1, 2, 3, 4, 5.0123, 6, 7},
		"G":    []float32{1, 2, 3, 4, 5, 6.8, 7},
		"H":    []float32{1, 2, 3, 4, 5, 6.00001, 7},
		"I":    []float32{1, 2, 3435.1111, 4, 5, 6, 7},
		"J":    []float32{1, 2, 3435.1111, 4, 5, 6, 7},
		"W":    []float32{1, 2, 3435.1111, 4, 5, 6, 7},
	})

	t.Logf("d.size = %v", d.Shape())
	d.Show()
}
