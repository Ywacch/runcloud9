package api

import (
	"errors"
	"net/http"
)

const maxJSONBodyBytes int64 = 1 << 20 // 1 MiB

func limitJSONBody(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxJSONBodyBytes)
}

func isBodyTooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}
