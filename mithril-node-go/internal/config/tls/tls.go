package configtls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type Client struct {
	Cert     *Cert `koanf:"cert"`
	Key      *Key  `koanf:"key"`
	ServerCA *Cert `koanf:"ca"`
}

type Server struct {
	Cert     *Cert `koanf:"cert"`
	Key      *Key  `koanf:"key"`
	ClientCA *Cert `koanf:"ca"`
}

type Cert struct {
	File string `koanf:"file"`
}

type Key struct {
	File string `koanf:"file"`
}

func TLSConfigWithClient(c *Client) (*tls.Config, error) {
	tlsCfg := &tls.Config{}

	// Load client certificate if provided
	if c.Cert.File != "" && c.Key.File != "" {
		clientCertificate, err := tls.LoadX509KeyPair(c.Cert.File, c.Key.File)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{clientCertificate}
	}

	if c.ServerCA.File != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(c.ServerCA.File)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file %q: %w", c.ServerCA.File, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid PEM in CA file %q", c.ServerCA.File)
		}

		tlsCfg.RootCAs = pool
	} else {
		pool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("failed to load system cert pool: %w", err)
		}

		tlsCfg.RootCAs = pool
	}

	return tlsCfg, nil
}

func TLSConfigWithServer(s *Server) (*tls.Config, error) {
	serverCertificate, err := tls.LoadX509KeyPair(s.Cert.File, s.Key.File)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCertificate},
	}

	if s.ClientCA.File != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(s.ClientCA.File)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA file %q: %w", s.ClientCA.File, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid PEM in client CA file %q", s.ClientCA.File)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsCfg.ClientAuth = tls.NoClientCert
	}

	return tlsCfg, nil
}
