package gframe

import "math"

type Settings struct {
	ThreadNum           int32
	OrderBins           int32
	FloatPrecision      int32
	ExploringDepth      int32
	Profiling           bool
	Debug               bool
	orderGroupThreshold int32
}

func (s *Settings) init() {
	s.orderGroupThreshold = int32(math.Pow(2, float64(s.OrderBins)))
}

func DefaultSettings() Settings {
	ret := Settings{
		ThreadNum:      8,
		OrderBins:      16,
		FloatPrecision: 6,
		ExploringDepth: 100,
		Profiling:      false,
	}
	ret.init()
	return ret
}

var gSettings = DefaultSettings()

func Configure(s Settings) {
	s.init()
	gSettings = s
}
