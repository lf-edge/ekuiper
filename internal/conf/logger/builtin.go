// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is taken from the log/syslog in the standard lib.
// However, there is a bug with overwhelming syslog that causes writes
// to block indefinitely. This is fixed by adding a write deadline.
// This also export the timestamp format to allow user to customize it.
//go:build !windows

package logger

import (
	"errors"
	"fmt"
	"log/syslog"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	severityMask   = 0x07
	facilityMask   = 0xf8
	localDeadline  = 20 * time.Millisecond
	remoteDeadline = 50 * time.Millisecond
)

// A SyslogWriter is a connection to a syslog server.
type SyslogWriter struct {
	priority syslog.Priority
	tag      string
	hostname string
	network  string
	raddr    string
	tsformat string

	mu   sync.Mutex // guards conn
	conn serverConn
}

// This interface and the separate syslog_unix.go file exist for
// Solaris support as implemented by gccgo.  On Solaris you can not
// simply open a TCP connection to the syslog daemon.  The gccgo
// sources have a syslog_solaris.go file that implements unixSyslog to
// return a type that satisfies this interface and simply calls the C
// library syslog function.
type serverConn interface {
	writeString(p syslog.Priority, hostname, tag, s, nl string) error
	close() error
}

type netConn struct {
	local    bool
	conn     net.Conn
	tsformat string
}

// Dial establishes a connection to a log daemon by connecting to
// address raddr on the specified network.  Each write to the returned
// writer sends a log message with the given facility, severity and
// tag.
// If network is empty, Dial will connect to the local syslog server.
func Dial(network, raddr string, priority syslog.Priority, tag, tsformat string) (*SyslogWriter, error) {
	if priority < 0 || priority > syslog.LOG_LOCAL7|syslog.LOG_DEBUG {
		return nil, errors.New("log/syslog: invalid priority")
	}

	if tag == "" {
		tag = os.Args[0]
	}
	hostname, _ := os.Hostname()

	w := &SyslogWriter{
		priority: priority,
		tag:      tag,
		hostname: hostname,
		network:  network,
		raddr:    raddr,
		tsformat: tsformat,
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.connect()
	if err != nil {
		return nil, err
	}
	return w, err
}

// connect makes a connection to the syslog server.
// It must be called with w.mu held.
func (w *SyslogWriter) connect() (err error) {
	if w.conn != nil {
		// ignore err from close, it makes sense to continue anyway
		w.conn.close()
		w.conn = nil
	}

	if w.network == "" {
		w.conn, err = unixSyslog()
		if w.hostname == "" {
			w.hostname = "localhost"
		}
	} else {
		var c net.Conn
		c, err = net.DialTimeout(w.network, w.raddr, remoteDeadline)
		if err == nil {
			w.conn = &netConn{conn: c, tsformat: w.tsformat}
			if w.hostname == "" {
				w.hostname = c.LocalAddr().String()
			}
		}
	}
	return
}

// Write sends a log message to the syslog daemon.
func (w *SyslogWriter) Write(b []byte) (int, error) {
	return w.writeAndRetry(w.priority, string(b))
}

// Close closes a connection to the syslog daemon.
func (w *SyslogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		err := w.conn.close()
		w.conn = nil
		return err
	}
	return nil
}

// Emerg logs a message with severity LOG_EMERG, ignoring the severity
// passed to New.
func (w *SyslogWriter) Emerg(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_EMERG, m)
	return err
}

// Alert logs a message with severity LOG_ALERT, ignoring the severity
// passed to New.
func (w *SyslogWriter) Alert(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_ALERT, m)
	return err
}

// Crit logs a message with severity LOG_CRIT, ignoring the severity
// passed to New.
func (w *SyslogWriter) Crit(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_CRIT, m)
	return err
}

// Err logs a message with severity LOG_ERR, ignoring the severity
// passed to New.
func (w *SyslogWriter) Err(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_ERR, m)
	return err
}

// Warning logs a message with severity LOG_WARNING, ignoring the
// severity passed to New.
func (w *SyslogWriter) Warning(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_WARNING, m)
	return err
}

// Notice logs a message with severity LOG_NOTICE, ignoring the
// severity passed to New.
func (w *SyslogWriter) Notice(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_NOTICE, m)
	return err
}

// Info logs a message with severity LOG_INFO, ignoring the severity
// passed to New.
func (w *SyslogWriter) Info(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_INFO, m)
	return err
}

// Debug logs a message with severity LOG_DEBUG, ignoring the severity
// passed to New.
func (w *SyslogWriter) Debug(m string) error {
	_, err := w.writeAndRetry(syslog.LOG_DEBUG, m)
	return err
}

func (w *SyslogWriter) writeAndRetry(p syslog.Priority, s string) (int, error) {
	pr := (w.priority & facilityMask) | (p & severityMask)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.conn != nil {
		if n, err := w.write(pr, s); err == nil {
			return n, err
		}
	}
	if err := w.connect(); err != nil {
		return 0, err
	}
	return w.write(pr, s)
}

// write generates and writes a syslog formatted string. The
// format is as follows: <PRI>TIMESTAMP HOSTNAME TAG[PID]: MSG
func (w *SyslogWriter) write(p syslog.Priority, msg string) (int, error) {
	// ensure it ends in a \n
	nl := ""
	if !strings.HasSuffix(msg, "\n") {
		nl = "\n"
	}

	err := w.conn.writeString(p, w.hostname, w.tag, msg, nl)
	if err != nil {
		return 0, err
	}
	// Note: return the length of the input, not the number of
	// bytes printed by Fprintf, because this must behave like
	// an io.Writer.
	return len(msg), nil
}

func (n *netConn) writeString(p syslog.Priority, hostname, tag, msg, nl string) error {
	timestamp := time.Now().Format(n.tsformat)
	if n.local {
		// Compared to the network form below, the changes are:
		//	Drop the hostname field from the Fprintf.
		n.conn.SetWriteDeadline(time.Now().Add(localDeadline))
		_, err := fmt.Fprintf(n.conn, "<%d>%s %s[%d]: %s%s",
			p, timestamp,
			tag, os.Getpid(), msg, nl)
		return err
	}

	n.conn.SetWriteDeadline(time.Now().Add(remoteDeadline))
	_, err := fmt.Fprintf(n.conn, "<%d>%s %s %s[%d]: %s%s",
		p, timestamp, hostname,
		tag, os.Getpid(), msg, nl)
	return err
}

func (n *netConn) close() error {
	return n.conn.Close()
}

// unixSyslog opens a connection to the syslog daemon running on the
// local machine using a Unix domain socket.
func unixSyslog() (conn serverConn, err error) {
	logTypes := []string{"unixgram", "unix"}
	logPaths := []string{"/dev/log", "/var/run/syslog", "/var/run/log"}
	for _, network := range logTypes {
		for _, path := range logPaths {
			conn, err := net.DialTimeout(network, path, localDeadline)
			if err != nil {
				continue
			} else {
				return &netConn{conn: conn, local: true}, nil
			}
		}
	}
	return nil, errors.New("Unix syslog delivery error")
}
