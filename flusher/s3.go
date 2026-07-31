package flusher

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// lengthPrefixSize is the width of the big-endian length prefix every
// Flusher's reader starts with (see Flusher.Flush); it mirrors
// buffer.bufferSizePadding.
const lengthPrefixSize = 8

// S3Flusher writes retired buffer data to S3. It holds a single *s3.Client
// for the life of the process, so every Flush call reuses that client's
// underlying HTTP connection pool instead of dialing a fresh connection per
// flush. Construct the client once (e.g. s3.NewFromConfig after
// config.LoadDefaultConfig) and share it across every S3Flusher/Buffer that
// needs one.
type S3Flusher struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Flusher builds a Flusher that uploads to bucket under prefix using
// client.
func NewS3Flusher(client *s3.Client, bucket, prefix string) *S3Flusher {
	return &S3Flusher{client: client, bucket: bucket, prefix: prefix}
}

// Flush uploads r to a key of the form <prefix>/<UnixNano>/data.bin, one
// object per call. r is expected to start with the 8-byte big-endian length
// prefix described on Flusher; reading it up front gives Flush the payload
// size without buffering or seeking, so the remaining bytes are streamed
// straight into the PutObject body via io.LimitReader -- r is only read,
// never copied or modified.
func (f *S3Flusher) Flush(r io.Reader) error {
	if r == nil {
		return nil
	}

	var lenPrefix [lengthPrefixSize]byte
	if _, err := io.ReadFull(r, lenPrefix[:]); err != nil {
		return fmt.Errorf("s3 flush: read length prefix: %w", err)
	}
	size := int64(binary.BigEndian.Uint64(lenPrefix[:]))

	key := f.prefix + "/" + strconv.FormatInt(time.Now().UnixNano(), 10) + "/data.bin"

	_, err := f.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:        aws.String(f.bucket),
		Key:           aws.String(key),
		Body:          io.LimitReader(r, size),
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("s3 flush: put %s: %w", key, err)
	}
	return nil
}
