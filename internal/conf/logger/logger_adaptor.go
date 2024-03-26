//go:build !edgex

package logger

import "github.com/sirupsen/logrus"

// with EdgeX enabled as of 2024, it includes OpenZiti support. OpenZiti uses the default
// logrus logger. This flag controls whether the logger should be quited or not and will
// only be enabled when the -tags edgex is supplied to the build
func adaptLogging(_ *logrus.Logger) {
	//no operation by default
}
