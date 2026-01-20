package io

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Config holds S3 configuration.
type S3Config struct {
	Region          string
	Endpoint        string // For MinIO or other S3-compatible services
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	ForcePathStyle  bool // Required for MinIO
}

// S3FileIO implements FileIO for S3.
type S3FileIO struct {
	client     *s3.Client
	properties map[string]string
}

// NewS3FileIO creates a new S3 file I/O handler.
func NewS3FileIO(ctx context.Context, cfg *S3Config) (*S3FileIO, error) {
	var opts []func(*config.LoadOptions) error

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		creds := credentials.NewStaticCredentialsProvider(
			cfg.AccessKeyID,
			cfg.SecretAccessKey,
			cfg.SessionToken,
		)
		opts = append(opts, config.WithCredentialsProvider(creds))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)

	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	if cfg.ForcePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3FileIO{
		client:     client,
		properties: make(map[string]string),
	}, nil
}

// parseS3URI parses an S3 URI into bucket and key.
func parseS3URI(uri string) (bucket, key string, err error) {
	// Handle both s3:// and s3a:// URIs
	uri = strings.TrimPrefix(uri, "s3a://")
	uri = strings.TrimPrefix(uri, "s3://")

	u, err := url.Parse("s3://" + uri)
	if err != nil {
		return "", "", fmt.Errorf("invalid S3 URI: %w", err)
	}

	bucket = u.Host
	key = strings.TrimPrefix(u.Path, "/")

	if bucket == "" {
		return "", "", fmt.Errorf("missing bucket in S3 URI")
	}

	return bucket, key, nil
}

// Open opens a file for reading.
func (s *S3FileIO) Open(ctx context.Context, path string) (InputFile, error) {
	bucket, key, err := parseS3URI(path)
	if err != nil {
		return nil, err
	}

	return &s3InputFile{
		client: s.client,
		bucket: bucket,
		key:    key,
		path:   path,
	}, nil
}

// Create creates a new file for writing.
func (s *S3FileIO) Create(ctx context.Context, path string) (OutputFile, error) {
	bucket, key, err := parseS3URI(path)
	if err != nil {
		return nil, err
	}

	return &s3OutputFile{
		client: s.client,
		bucket: bucket,
		key:    key,
		path:   path,
	}, nil
}

// Delete deletes a file.
func (s *S3FileIO) Delete(ctx context.Context, path string) error {
	bucket, key, err := parseS3URI(path)
	if err != nil {
		return err
	}

	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

// Exists checks if a file exists.
func (s *S3FileIO) Exists(ctx context.Context, path string) (bool, error) {
	bucket, key, err := parseS3URI(path)
	if err != nil {
		return false, err
	}

	_, err = s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a not found error
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") || strings.Contains(err.Error(), "NoSuchKey") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Properties returns the properties of this FileIO.
func (s *S3FileIO) Properties() map[string]string {
	return s.properties
}

// DeleteFiles deletes multiple files.
func (s *S3FileIO) DeleteFiles(ctx context.Context, paths []string) error {
	// Group by bucket
	bucketKeys := make(map[string][]string)
	for _, path := range paths {
		bucket, key, err := parseS3URI(path)
		if err != nil {
			return err
		}
		bucketKeys[bucket] = append(bucketKeys[bucket], key)
	}

	// Delete from each bucket
	for bucket, keys := range bucketKeys {
		for _, key := range keys {
			_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			if err != nil {
				return fmt.Errorf("failed to delete %s/%s: %w", bucket, key, err)
			}
		}
	}

	return nil
}

// ListFiles lists files under a prefix.
func (s *S3FileIO) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	bucket, key, err := parseS3URI(prefix)
	if err != nil {
		return nil, err
	}

	var files []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(key),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			files = append(files, fmt.Sprintf("s3://%s/%s", bucket, *obj.Key))
		}
	}

	return files, nil
}

// s3InputFile implements InputFile for S3.
type s3InputFile struct {
	client *s3.Client
	bucket string
	key    string
	path   string
}

func (f *s3InputFile) Location() string {
	return f.path
}

func (f *s3InputFile) Exists(ctx context.Context) (bool, error) {
	_, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "404") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (f *s3InputFile) Length(ctx context.Context) (int64, error) {
	resp, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})
	if err != nil {
		return 0, err
	}
	if resp.ContentLength != nil {
		return *resp.ContentLength, nil
	}
	return 0, nil
}

func (f *s3InputFile) Open(ctx context.Context) (io.ReadCloser, error) {
	resp, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (f *s3InputFile) OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	resp, err := f.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// s3OutputFile implements OutputFile for S3.
type s3OutputFile struct {
	client *s3.Client
	bucket string
	key    string
	path   string
}

func (f *s3OutputFile) Location() string {
	return f.path
}

func (f *s3OutputFile) Create(ctx context.Context) (io.WriteCloser, error) {
	// Check if file exists
	_, err := f.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(f.key),
	})
	if err == nil {
		return nil, fmt.Errorf("file already exists: %s", f.path)
	}

	return f.CreateOverwrite(ctx)
}

func (f *s3OutputFile) CreateOverwrite(ctx context.Context) (io.WriteCloser, error) {
	return &s3Writer{
		client: f.client,
		bucket: f.bucket,
		key:    f.key,
		buffer: new(bytes.Buffer),
		ctx:    ctx,
	}, nil
}

func (f *s3OutputFile) ToInputFile() InputFile {
	return &s3InputFile{
		client: f.client,
		bucket: f.bucket,
		key:    f.key,
		path:   f.path,
	}
}

// s3Writer buffers writes and uploads on close.
type s3Writer struct {
	client *s3.Client
	bucket string
	key    string
	buffer *bytes.Buffer
	ctx    context.Context
}

func (w *s3Writer) Write(p []byte) (n int, err error) {
	return w.buffer.Write(p)
}

func (w *s3Writer) Close() error {
	_, err := w.client.PutObject(w.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   bytes.NewReader(w.buffer.Bytes()),
	})
	return err
}
