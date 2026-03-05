package configpprof

type Server struct {
	Listen string `koanf:"listen" default:"[::]:8080"`
}
