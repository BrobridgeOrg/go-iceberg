package io

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// LocalFileIO implements FileIO for local filesystem.
type LocalFileIO struct {
	properties map[string]string
}

// NewLocalFileIO creates a new local file I/O handler.
func NewLocalFileIO() *LocalFileIO {
	return &LocalFileIO{
		properties: make(map[string]string),
	}
}

// Open opens a file for reading.
func (l *LocalFileIO) Open(ctx context.Context, path string) (InputFile, error) {
	path = normalizePath(path)
	return &localInputFile{path: path}, nil
}

// Create creates a new file for writing.
func (l *LocalFileIO) Create(ctx context.Context, path string) (OutputFile, error) {
	path = normalizePath(path)
	return &localOutputFile{path: path}, nil
}

// Delete deletes a file.
func (l *LocalFileIO) Delete(ctx context.Context, path string) error {
	path = normalizePath(path)
	return os.Remove(path)
}

// Exists checks if a file exists.
func (l *LocalFileIO) Exists(ctx context.Context, path string) (bool, error) {
	path = normalizePath(path)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Properties returns the properties of this FileIO.
func (l *LocalFileIO) Properties() map[string]string {
	return l.properties
}

// DeleteFiles deletes multiple files.
func (l *LocalFileIO) DeleteFiles(ctx context.Context, paths []string) error {
	for _, path := range paths {
		if err := l.Delete(ctx, path); err != nil {
			return fmt.Errorf("failed to delete %s: %w", path, err)
		}
	}
	return nil
}

// ListFiles lists files under a prefix.
func (l *LocalFileIO) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	prefix = normalizePath(prefix)
	var files []string

	err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// normalizePath removes file:// prefix if present.
func normalizePath(path string) string {
	if strings.HasPrefix(path, "file://") {
		return strings.TrimPrefix(path, "file://")
	}
	return path
}

// localInputFile implements InputFile for local filesystem.
type localInputFile struct {
	path string
}

func (f *localInputFile) Location() string {
	return f.path
}

func (f *localInputFile) Exists(ctx context.Context) (bool, error) {
	_, err := os.Stat(f.path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (f *localInputFile) Length(ctx context.Context) (int64, error) {
	info, err := os.Stat(f.path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (f *localInputFile) Open(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(f.path)
}

func (f *localInputFile) OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error) {
	file, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &limitedReadCloser{
		Reader: io.LimitReader(file, length),
		Closer: file,
	}, nil
}

// localOutputFile implements OutputFile for local filesystem.
type localOutputFile struct {
	path string
}

func (f *localOutputFile) Location() string {
	return f.path
}

func (f *localOutputFile) Create(ctx context.Context) (io.WriteCloser, error) {
	// Ensure parent directory exists
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create file with O_EXCL to fail if exists
	return os.OpenFile(f.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
}

func (f *localOutputFile) CreateOverwrite(ctx context.Context) (io.WriteCloser, error) {
	// Ensure parent directory exists
	dir := filepath.Dir(f.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	return os.Create(f.path)
}

func (f *localOutputFile) ToInputFile() InputFile {
	return &localInputFile{path: f.path}
}

// limitedReadCloser wraps a limited reader with a closer.
type limitedReadCloser struct {
	io.Reader
	io.Closer
}
