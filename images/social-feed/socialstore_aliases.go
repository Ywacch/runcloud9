package main

import "github.com/ywacch/cloud9/images/social-feed/socialstore"

type (
	User            = socialstore.User
	Post            = socialstore.Post
	Thread          = socialstore.Thread
	FeedItem        = socialstore.FeedItem
	ThreadPoolEntry = socialstore.ThreadPoolEntry
	SocialStore     = socialstore.Store
)

var (
	ErrNotFound      = socialstore.ErrNotFound
	ErrUsernameTaken = socialstore.ErrUsernameTaken
)

var _ socialstore.Store = (*DB)(nil)
