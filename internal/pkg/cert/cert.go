package cert

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/lf-edge/ekuiper/internal/conf"
)

type TlsConfigurationOptions struct {
	SkipCertVerify bool
	CertFile       string
	KeyFile        string
	CaFile         string
}

func GenerateTLSForClient(
	Opts TlsConfigurationOptions) (*tls.Config, error) {

	tlsConfig := &tls.Config{
		InsecureSkipVerify: Opts.SkipCertVerify,
	}

	if len(Opts.CertFile) <= 0 && len(Opts.KeyFile) <= 0 {
		tlsConfig.Certificates = nil
	} else {
		if cert, err := certLoader(Opts.CertFile, Opts.KeyFile); err != nil {
			return nil, err
		} else {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}

	if len(Opts.CaFile) > 0 {
		root, err := caLoader(Opts.CaFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = root
	}

	return tlsConfig, nil
}

func certLoader(certFilePath, keyFilePath string) (tls.Certificate, error) {
	if cp, err := conf.ProcessPath(certFilePath); err == nil {
		if kp, err1 := conf.ProcessPath(keyFilePath); err1 == nil {
			if cer, err2 := tls.LoadX509KeyPair(cp, kp); err2 != nil {
				return tls.Certificate{}, err2
			} else {
				return cer, nil
			}
		} else {
			return tls.Certificate{}, err1
		}
	} else {
		return tls.Certificate{}, err
	}
}

func caLoader(caFilePath string) (*x509.CertPool, error) {
	if cp, err := conf.ProcessPath(caFilePath); err == nil {
		pool := x509.NewCertPool()
		caCrt, err1 := os.ReadFile(cp)
		if err1 != nil {
			return nil, err1
		}
		pool.AppendCertsFromPEM(caCrt)
		return pool, err1
	} else {
		return nil, err
	}
}
