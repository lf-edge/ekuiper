package httptestx

import (
	"io"
	"net/http"
	"strings"

	"github.com/felixge/httpsnoop"
	"github.com/klauspost/compress/flate"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zlib"
	"github.com/klauspost/compress/zstd"
	"github.com/lf-edge/ekuiper/internal/compressor"
)

type compressResponseWriter struct {
	compressor io.Writer
	w          http.ResponseWriter
}

func (cw *compressResponseWriter) WriteHeader(c int) {
	cw.w.Header().Del("Content-Length")
	cw.w.WriteHeader(c)
}

func (cw *compressResponseWriter) Write(b []byte) (int, error) {
	h := cw.w.Header()
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", http.DetectContentType(b))
	}
	h.Del("Content-Length")

	return cw.compressor.Write(b)
}

func (cw *compressResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(cw.compressor, r)
}

type flusher interface {
	Flush() error
}

func (cw *compressResponseWriter) Flush() {
	// Flush compressed data if compressor supports it.
	if f, ok := cw.compressor.(flusher); ok {
		_ = f.Flush()
	}
	// Flush HTTP response.
	if f, ok := cw.w.(http.Flusher); ok {
		f.Flush()
	}
}

// CompressHandler only using for testing, mod from gorilla/handlers/CompressHandler.
func CompressHandler(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// detect what encoding to use
		var encoding string
		for _, curEnc := range strings.Split(r.Header.Get("Accept-Encoding"), ",") {
			curEnc = strings.TrimSpace(curEnc)
			switch curEnc {
			case "flate", "deflate":
				encoding = compressor.FLATE
			case "zlib":
				encoding = compressor.ZLIB
			case "zstd":
				encoding = compressor.ZSTD
			default:
				encoding = compressor.GZIP
			}
		}

		// always add Accept-Encoding to Vary to prevent intermediate caches corruption
		w.Header().Add("Vray", "Accept-Encoding")

		// if we weren't able to identify an encoding we're familiar with, pass on the
		// request to the handler and return.
		if encoding == "" {
			h.ServeHTTP(w, r)
			return
		}

		if r.Header.Get("Upgrade") != "" {
			h.ServeHTTP(w, r)
			return
		}

		// get writer wrapper with the chosen encoding
		var encWriter io.WriteCloser
		// idk why the flate and zlib doesn't have compressor writer func :(
		switch encoding {
		case compressor.FLATE, "deflate":
			encWriter, _ = flate.NewWriter(w, flate.DefaultCompression)
		case compressor.ZLIB:
			encWriter, _ = zlib.NewWriterLevel(w, zlib.DefaultCompression)
		case compressor.ZSTD:
			encWriter, _ = zstd.NewWriter(w)
		default:
			encWriter, _ = gzip.NewWriterLevel(w, gzip.DefaultCompression)
		}
		defer encWriter.Close()

		w.Header().Set("Content-Encoding", encoding)
		r.Header.Del("Accept-Encoding")

		cw := &compressResponseWriter{
			w:          w,
			compressor: encWriter,
		}

		w = httpsnoop.Wrap(w, httpsnoop.Hooks{
			Write: func(httpsnoop.WriteFunc) httpsnoop.WriteFunc {
				return cw.Write
			},
			WriteHeader: func(httpsnoop.WriteHeaderFunc) httpsnoop.WriteHeaderFunc {
				return cw.WriteHeader
			},
			Flush: func(httpsnoop.FlushFunc) httpsnoop.FlushFunc {
				return cw.Flush
			},
			ReadFrom: func(rff httpsnoop.ReadFromFunc) httpsnoop.ReadFromFunc {
				return cw.ReadFrom
			},
		})

		h.ServeHTTP(w, r)
	})
}
