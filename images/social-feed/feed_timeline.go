package main

import (
	"encoding/json"

	"github.com/ywacch/cloud9/pkg/templateapp/jobs/timeline"
)

func feedItemsFromTimeline(items []timeline.Item) []FeedItem {
	out := make([]FeedItem, 0, len(items))
	for _, it := range items {
		var fi FeedItem
		if len(it.Payload) > 0 {
			if err := json.Unmarshal(it.Payload, &fi); err != nil {
				continue
			}
		}
		if fi.PostID == 0 && it.PostID != 0 {
			fi.PostID = int(it.PostID)
		}
		if fi.AuthorID == 0 && it.AuthorID != 0 {
			fi.AuthorID = int(it.AuthorID)
		}
		if fi.CreatedAt.IsZero() && !it.CreatedAt.IsZero() {
			fi.CreatedAt = it.CreatedAt
		}
		out = append(out, fi)
	}
	return out
}
