package configadvertisement

type AdvertisementConfig struct {
	GRPC GRPCConfig `koanf:"grpc"`
}

type GRPCConfig struct {
	URLs []string `koanf:"urls"`
}
