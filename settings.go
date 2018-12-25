package gframe

type Settings struct {
	ThreadNum     int32
	HistogramBins int32
}

var DefaultSettings = Settings{
	ThreadNum:     8,
	HistogramBins: 16,
}

var gSettings = DefaultSettings

func Configure(s Settings) {
	gSettings = s
}
