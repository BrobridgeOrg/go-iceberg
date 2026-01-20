package io

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestLocalFileIO_CreateAndOpen(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	testPath := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("Hello, Iceberg!")

	// Test Create
	outputFile, err := fileIO.Create(ctx, testPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Test Write
	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		t.Fatalf("CreateOverwrite failed: %v", err)
	}

	n, err := writer.Write(testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != len(testContent) {
		t.Errorf("Write n = %d, want %d", n, len(testContent))
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer failed: %v", err)
	}

	// Test Open
	inputFile, err := fileIO.Open(ctx, testPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Test Length
	length, err := inputFile.Length(ctx)
	if err != nil {
		t.Fatalf("Length failed: %v", err)
	}
	if length != int64(len(testContent)) {
		t.Errorf("Length = %d, want %d", length, len(testContent))
	}

	// Test Read
	reader, err := inputFile.Open(ctx)
	if err != nil {
		t.Fatalf("Open reader failed: %v", err)
	}

	readContent := make([]byte, len(testContent))
	n, err = reader.Read(readContent)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != len(testContent) {
		t.Errorf("Read n = %d, want %d", n, len(testContent))
	}

	if !bytes.Equal(readContent, testContent) {
		t.Errorf("Content mismatch: got %s, want %s", readContent, testContent)
	}

	if err := reader.Close(); err != nil {
		t.Fatalf("Close reader failed: %v", err)
	}
}

func TestLocalFileIO_Delete(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	testPath := filepath.Join(tmpDir, "delete_test.txt")

	// Create file first
	if err := os.WriteFile(testPath, []byte("test"), 0644); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(testPath); os.IsNotExist(err) {
		t.Fatal("Test file should exist")
	}

	// Test Delete
	if err := fileIO.Delete(ctx, testPath); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify file is gone
	if _, err := os.Stat(testPath); !os.IsNotExist(err) {
		t.Error("File should be deleted")
	}
}

func TestLocalFileIO_Exists(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	existingPath := filepath.Join(tmpDir, "exists.txt")
	nonExistingPath := filepath.Join(tmpDir, "not_exists.txt")

	// Create one file
	if err := os.WriteFile(existingPath, []byte("test"), 0644); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Test existing file
	exists, err := fileIO.Exists(ctx, existingPath)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}

	// Test non-existing file
	exists, err = fileIO.Exists(ctx, nonExistingPath)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("File should not exist")
	}
}

func TestLocalFileIO_CreateDirectories(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()

	// Create file in nested directory that doesn't exist
	nestedPath := filepath.Join(tmpDir, "a", "b", "c", "test.txt")

	outputFile, err := fileIO.Create(ctx, nestedPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		t.Fatalf("CreateOverwrite failed: %v", err)
	}

	if _, err := writer.Write([]byte("test")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(nestedPath); os.IsNotExist(err) {
		t.Error("File should exist in nested directory")
	}
}

func TestLocalFileIO_Location(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	testPath := filepath.Join(tmpDir, "location_test.txt")

	// Create and write a test file
	if err := os.WriteFile(testPath, []byte("test"), 0644); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	// Open file and check location
	inputFile, err := fileIO.Open(ctx, testPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	location := inputFile.Location()
	if location != testPath {
		t.Errorf("Location = %s, want %s", location, testPath)
	}
}

func TestLocalFileIO_MultipleWrites(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	testPath := filepath.Join(tmpDir, "multi_write.txt")

	// First write
	outputFile, err := fileIO.Create(ctx, testPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		t.Fatalf("CreateOverwrite failed: %v", err)
	}

	content1 := []byte("First content")
	if _, err := writer.Write(content1); err != nil {
		t.Fatalf("First write failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Second write (should overwrite)
	outputFile2, err := fileIO.Create(ctx, testPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	writer2, err := outputFile2.CreateOverwrite(ctx)
	if err != nil {
		t.Fatalf("CreateOverwrite failed: %v", err)
	}

	content2 := []byte("Second content - longer")
	if _, err := writer2.Write(content2); err != nil {
		t.Fatalf("Second write failed: %v", err)
	}
	if err := writer2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify content is from second write
	data, err := os.ReadFile(testPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !bytes.Equal(data, content2) {
		t.Errorf("Content = %s, want %s", data, content2)
	}
}

func TestLocalFileIO_EmptyFile(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	fileIO := NewLocalFileIO()
	testPath := filepath.Join(tmpDir, "empty.txt")

	// Create empty file
	outputFile, err := fileIO.Create(ctx, testPath)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	writer, err := outputFile.CreateOverwrite(ctx)
	if err != nil {
		t.Fatalf("CreateOverwrite failed: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Open and check length
	inputFile, err := fileIO.Open(ctx, testPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	length, err := inputFile.Length(ctx)
	if err != nil {
		t.Fatalf("Length failed: %v", err)
	}
	if length != 0 {
		t.Errorf("Length = %d, want 0", length)
	}
}
