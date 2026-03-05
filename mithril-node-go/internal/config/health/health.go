package confighealth

type Server struct {
	Listen string `koanf:"listen" default:"[::]:6060"`
}
