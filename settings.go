package gframe

type Settings struct {
	ThreadNum      int32
	HistogramBins  int32
	FloatPrecision int32
	ExploringDepth int32
	Profiling      bool
}

var DefaultSettings = Settings{
	ThreadNum:      8,
	HistogramBins:  16,
	FloatPrecision: 6,
	ExploringDepth: 100,
	Profiling:      false,
}

var gSettings = DefaultSettings

func Configure(s Settings) {
	gSettings = s
}
