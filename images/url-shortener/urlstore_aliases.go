package main

import "github.com/ywacch/cloud9/images/url-shortener/urlstore"

type (
	URLRecord = urlstore.Record
	URLStats  = urlstore.Record
	URLStore  = urlstore.Store
)

var _ urlstore.Store = (*DB)(nil)
