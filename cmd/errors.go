package cmd

import "errors"

var (
	ErrBucketNotFound  = errors.New("bucket not found")
	ErrObjectNotFound  = errors.New("object not found")
	ErrBucketExists    = errors.New("bucket already exists")
	ErrInvalidArgument = errors.New("invalid argument")
)
