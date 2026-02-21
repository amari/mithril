package config

type TLSClientConfig struct {
	Cert *TLSCert `koanf:"cert"`
	Key  *TLSKey  `koanf:"key"`

	// Server CA File is the path to a PEM-encoded file containing one or more trusted
	// root or intermediate certificates. These certificates are used to verify the
	// server's certificate during the TLS handshake.
	//
	//
	// This field configures *trust*, not identity.
	CA *TLSCA `koanf:"ca"`
}

type TLSServerConfig struct {
	Cert *TLSCert `koanf:"cert"`
	Key  *TLSKey  `koanf:"key"`

	// Client CA File is the path to a PEM-encoded file containing one or more trusted
	// root or intermediate certificates. These certificates are used to verify the
	// client's certificate during the TLS handshake.
	//
	//
	// This field configures *trust*, not identity.
	CA *TLSCA `koanf:"ca"`
}

type TLSCert struct {
	File string `koanf:"file"`
}

type TLSKey struct {
	File string `koanf:"file"`
}

type TLSCA struct {
	File string `koanf:"file"`
}
