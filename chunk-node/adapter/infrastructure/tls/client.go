package tls

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// ClientConfig holds TLS configuration for a client connecting to a server.
type ClientConfig struct {
	// ServerCAFile is the path to a PEM-encoded file containing one or more trusted
	// root or intermediate certificates. These certificates are used to verify the
	// server's certificate during the TLS handshake.
	//
	//
	// This field configures *trust*, not identity.
	ServerCAFile string `koanf:"serverCAFile"`

	// CertFile is the path to the client's own certificate (PEM-encoded). If set,
	// it will be presented to the server during the TLS handshake for client
	// authentication (mTLS).
	//
	// Optional — omit if client authentication is not required.
	CertFile string `koanf:"certFile"`

	// KeyFile is the path to the private key (PEM-encoded) corresponding to CertFile.
	// Required if CertFile is set.
	//
	// This key should be protected and never shared between components.
	KeyFile string `koanf:"keyFile"`
}

func (c *ClientConfig) Validate() error {
	if c.CertFile != "" && c.KeyFile == "" {
		return fmt.Errorf("keyFile must be set if certFile is set")
	}

	return nil
}

func ClientConfigToTLSConfig(cfg *ClientConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{}

	// Load client certificate if provided
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		clientCertificate, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{clientCertificate}
	}

	if cfg.ServerCAFile != "" {
		pool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.ServerCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file %q: %w", cfg.ServerCAFile, err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid PEM in CA file %q", cfg.ServerCAFile)
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
