package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ServerConfig holds TLS configuration for a server accepting client connections.
type ServerConfig struct {
	// ServerCAFile is the path to a PEM-encoded file containing one or more trusted
	// root or intermediate certificates. These certificates are used to verify the
	// client's certificate during the TLS handshake.
	//
	//
	// This field configures *trust*, not identity.
	ClientCAFile string `koanf:"clientCAFile"`

	// CertFile is the path to the Server's own certificate (PEM-encoded). If set,
	// it will be presented to the server during the TLS handshake for Server
	// authentication (mTLS).
	CertFile string `koanf:"certFile"`

	// KeyFile is the path to the private key (PEM-encoded) corresponding to CertFile.
	//
	// This key should be protected and never shared between components.
	KeyFile string `koanf:"keyFile"`
}

func (c *ServerConfig) Validate() error {
	if c.CertFile == "" {
		return fmt.Errorf("certFile must be set for server TLS configuration")
	}

	if c.KeyFile == "" {
		return fmt.Errorf("keyFile must be set for server TLS configuration")
	}

	return nil
}

func ServerConfigToTLSConfig(cfg *ServerConfig) (*tls.Config, error) {
	serverCertificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCertificate},
	}

	if cfg.ClientCAFile != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.ClientCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read client CA file %q: %w", cfg.ClientCAFile, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid PEM in client CA file %q", cfg.ClientCAFile)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		tlsCfg.ClientAuth = tls.NoClientCert
	}

	return tlsCfg, nil
}
