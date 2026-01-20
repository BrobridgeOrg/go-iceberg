// Package io provides file I/O interfaces for Iceberg tables.
package io

import (
	"context"
	"io"
)

// FileIO is the interface for file operations.
type FileIO interface {
	// Open opens a file for reading.
	Open(ctx context.Context, path string) (InputFile, error)

	// Create creates a new file for writing.
	Create(ctx context.Context, path string) (OutputFile, error)

	// Delete deletes a file.
	Delete(ctx context.Context, path string) error

	// Exists checks if a file exists.
	Exists(ctx context.Context, path string) (bool, error)

	// Properties returns the properties of this FileIO.
	Properties() map[string]string
}

// InputFile represents a readable file.
type InputFile interface {
	// Location returns the file location.
	Location() string

	// Exists checks if the file exists.
	Exists(ctx context.Context) (bool, error)

	// Length returns the file length in bytes.
	Length(ctx context.Context) (int64, error)

	// Open opens the file for reading.
	Open(ctx context.Context) (io.ReadCloser, error)

	// OpenRange opens a range of the file for reading.
	OpenRange(ctx context.Context, offset, length int64) (io.ReadCloser, error)
}

// OutputFile represents a writable file.
type OutputFile interface {
	// Location returns the file location.
	Location() string

	// Create creates the file for writing.
	Create(ctx context.Context) (io.WriteCloser, error)

	// CreateOverwrite creates or overwrites the file.
	CreateOverwrite(ctx context.Context) (io.WriteCloser, error)

	// ToInputFile converts this to an InputFile after writing.
	ToInputFile() InputFile
}

// SeekableReader extends io.ReadCloser with seeking capabilities.
type SeekableReader interface {
	io.ReadCloser
	io.Seeker
	io.ReaderAt
}

// BulkFileIO extends FileIO with bulk operations.
type BulkFileIO interface {
	FileIO

	// DeleteFiles deletes multiple files.
	DeleteFiles(ctx context.Context, paths []string) error

	// ListFiles lists files under a prefix.
	ListFiles(ctx context.Context, prefix string) ([]string, error)
}
