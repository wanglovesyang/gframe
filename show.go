package gframe

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

func (d *DataFrame) Show() {
	termSize, err := getTermSize()
	if err != nil {
		log.Printf("fail to get term size, %v", err)
		termSize = [2]int32{300, 20}
	}

	//log.Printf("term_size=%v", termSize)
	leakSizeY, leakSizeX := calcLeakSize(termSize)
	//log.Printf("leak_size=[%d, %d]", leakSizeY, leakSizeX)

	widths := make([]int32, len(d.cols)+1)
	rawMtx := make([][]string, len(d.cols)+1)
	for i, col := range d.cols {
		rawMtx[i+1] = d.renderColumn(col, leakSizeY)
		widths[i+1] = maxShownWidth(rawMtx[i+1])
	}
	rawMtx[0] = d.mockIdColumns(leakSizeY)
	widths[0] = maxShownWidth(rawMtx[0])

	leftEndCol := 0
	offset := int32(0)
	for i := 0; i < len(rawMtx); i++ {
		offset += widths[i] + 1
		leftEndCol = i
		if offset > leakSizeX {
			break
		}
	}

	rightEndBeg := 0
	offset = 0
	for i := len(rawMtx) - 1; i >= 0; i-- {
		offset += widths[i] + 1
		rightEndBeg = i
		if offset > leakSizeX {
			break
		}
	}
	rightEndBeg++

	//log.Printf("leftEnd = %d, rightEnd = %d", leftEndCol, rightEndBeg)
	if leftEndCol < rightEndBeg {
		left := rawMtx[0:leftEndCol]
		right := rawMtx[rightEndBeg:]
		leftWidth := widths[0:leftEndCol]
		rightWidth := widths[rightEndBeg:]
		left = append(left, d.mockDotColumns(int32(len(left[0]))))
		leftWidth = append(leftWidth, 3)
		rawMtx = append(left, right...)
		widths = append(leftWidth, rightWidth...)
	}

	//log.Printf("size of mtx = %d, size of width = %d", len(rawMtx), len(widths))
	//log.Printf("size of mtx[0] = %d, mtx[1] = %d", len(rawMtx[0]), len(rawMtx[1]))

	lineBuf := make([]string, len(rawMtx))
	for i := 0; i < len(rawMtx[0]); i++ {
		for j := 0; j < len(rawMtx); j++ {
			lineBuf[j] = padding(rawMtx[j][i], widths[j])
		}
		fmt.Printf("%s\n", strings.Join(lineBuf, " "))
	}
}

func padding(s string, maxLen int32) string {
	base := make([]rune, maxLen)
	for i := 0; i < int(maxLen); i++ {
		base[i] = ' '
	}
	copy(base, []rune(s))
	return string(base)
}

func calcLeakSize(tSize [2]int32) (retY int32, retX int32) {
	return (tSize[0] - 5) / 2, (tSize[1] - 2 - int32(len("...")) - 2) / 2
}

func (d *DataFrame) mockDotColumns(l int32) (ret []string) {
	ret = make([]string, l)
	for i := int32(0); i < l; i++ {
		ret[i] = "..."
	}

	return
}

func (d *DataFrame) mockIdColumns(leakSize int32) (ret []string) {
	if 2*leakSize+1 >= int32(d.shape[0]) {
		ret = make([]string, d.shape[0]+1)
		ret[0] = ""
		for i := 0; i < d.shape[0]; i++ {
			ret[i+1] = strconv.FormatInt(int64(i), 32)
		}
	} else {
		ret = make([]string, 2*leakSize+2)
		ret[0] = ""
		for i := 0; i < int(leakSize); i++ {
			ret[i+1] = strconv.FormatInt(int64(i), 32)
		}
		ret[leakSize+1] = "..."
		for i := 0; i < int(leakSize); i++ {
			ret[len(ret)-1-i] = strconv.FormatInt(int64(d.shape[0]-i-1), 32)
		}
	}

	return
}

func maxShownWidth(col []string) (maxLen int32) {
	for _, v := range col {
		if len(v) > int(maxLen) {
			maxLen = int32(len(v))
		}
	}

	return
}

func precFormat(v float32) (ret string) {
	ret = strconv.FormatFloat(float64(v), 'f', int(gSettings.FloatPrecision), 32)
	ret = strings.TrimRight(ret, "0")
	ret = strings.TrimRight(ret, ".")
	return
}

func (d *DataFrame) renderColumn(ent ColEntry, leakSize int32) (ret []string) {
	if 2*leakSize+1 >= int32(d.shape[0]) {
		ret = make([]string, d.shape[0]+1)
		ret[0] = ent.Name
		if ent.tp == String {
			for i, v := range d.idCols[ent.id] {
				ret[i+1] = v
			}
		} else if ent.tp == Float32 {
			for i, v := range d.valCols[ent.id] {
				ret[i+1] = precFormat(v)
			}
		}
	} else {
		ret = make([]string, 2*leakSize+2)
		ret[0] = ent.Name
		if ent.tp == String {
			for i, v := range d.idCols[ent.id][0:leakSize] {
				ret[i+1] = v
			}

			ret[leakSize+1] = "..."

			for i, v := range d.idCols[ent.id][d.shape[0]-int(leakSize):] {
				ret[i+int(leakSize)+2] = v
			}
		} else if ent.tp == Float32 {
			for i, v := range d.valCols[ent.id][0:leakSize] {
				ret[i+1] = precFormat(v)
			}

			ret[leakSize+1] = "..."

			for i, v := range d.valCols[ent.id][d.shape[0]-int(leakSize):] {
				ret[i+int(leakSize)+2] = precFormat(v)
			}
		}
	}

	return
}
