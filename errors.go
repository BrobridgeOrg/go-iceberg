package goiceberg

import (
	"errors"
	"fmt"
	"time"
)

// Common errors for go-iceberg operations.
var (
	// Table errors
	ErrTableNotFound      = errors.New("table not found")
	ErrTableAlreadyExists = errors.New("table already exists")

	// Namespace errors
	ErrNamespaceNotFound      = errors.New("namespace not found")
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrNamespaceNotEmpty      = errors.New("namespace is not empty")

	// Transaction errors
	ErrTransactionClosed  = errors.New("transaction already committed or aborted")
	ErrCommitConflict     = errors.New("commit conflict: table was modified")
	ErrRequirementFailed  = errors.New("commit requirement failed")
	ErrNoChangesToCommit  = errors.New("no changes to commit")

	// Schema errors
	ErrSchemaNotCompatible = errors.New("schema is not compatible")
	ErrColumnNotFound      = errors.New("column not found")
	ErrInvalidSchema       = errors.New("invalid schema")

	// Snapshot errors
	ErrSnapshotNotFound = errors.New("snapshot not found")
	ErrNoCurrentSnapshot = errors.New("table has no current snapshot")

	// Data errors
	ErrInvalidData   = errors.New("invalid data format")
	ErrTypeMismatch  = errors.New("type mismatch")
	ErrInvalidFilter = errors.New("invalid filter expression")

	// IO errors
	ErrFileNotFound  = errors.New("file not found")
	ErrIOFailed      = errors.New("IO operation failed")
	ErrInvalidPath   = errors.New("invalid file path")

	// Catalog errors
	ErrCatalogNotConfigured = errors.New("catalog not configured")
	ErrAuthenticationFailed = errors.New("authentication failed")

	// Configuration errors
	ErrInvalidConfig = errors.New("invalid configuration")
)

// CommitConflictError represents a commit conflict with details.
type CommitConflictError struct {
	TableIdentifier string
	ExpectedVersion int64
	ActualVersion   int64
	Cause           error
}

// Error implements the error interface.
func (e *CommitConflictError) Error() string {
	return fmt.Sprintf("commit conflict on table %s: expected version %d, actual version %d",
		e.TableIdentifier, e.ExpectedVersion, e.ActualVersion)
}

// Unwrap returns the underlying cause.
func (e *CommitConflictError) Unwrap() error {
	return e.Cause
}

// Is reports whether the target matches this error.
func (e *CommitConflictError) Is(target error) bool {
	return target == ErrCommitConflict
}

// RequirementError represents a failed commit requirement.
type RequirementError struct {
	Requirement string
	Expected    any
	Actual      any
}

// Error implements the error interface.
func (e *RequirementError) Error() string {
	return fmt.Sprintf("requirement failed: %s (expected: %v, actual: %v)",
		e.Requirement, e.Expected, e.Actual)
}

// Is reports whether the target matches this error.
func (e *RequirementError) Is(target error) bool {
	return target == ErrRequirementFailed
}

// RetryableError wraps an error that can be retried.
type RetryableError struct {
	Cause      error
	RetryAfter time.Duration
}

// Error implements the error interface.
func (e *RetryableError) Error() string {
	if e.RetryAfter > 0 {
		return fmt.Sprintf("retryable error (retry after %v): %v", e.RetryAfter, e.Cause)
	}
	return fmt.Sprintf("retryable error: %v", e.Cause)
}

// Unwrap returns the underlying cause.
func (e *RetryableError) Unwrap() error {
	return e.Cause
}

// IsRetryable reports whether the error can be retried.
func IsRetryable(err error) bool {
	var retryable *RetryableError
	if errors.As(err, &retryable) {
		return true
	}
	// Commit conflicts are retryable
	var conflict *CommitConflictError
	return errors.As(err, &conflict)
}

// TableNotFoundError represents a table not found error with details.
type TableNotFoundError struct {
	Namespace string
	TableName string
}

// Error implements the error interface.
func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("table not found: %s.%s", e.Namespace, e.TableName)
}

// Is reports whether the target matches this error.
func (e *TableNotFoundError) Is(target error) bool {
	return target == ErrTableNotFound
}

// NamespaceNotFoundError represents a namespace not found error with details.
type NamespaceNotFoundError struct {
	Namespace string
}

// Error implements the error interface.
func (e *NamespaceNotFoundError) Error() string {
	return fmt.Sprintf("namespace not found: %s", e.Namespace)
}

// Is reports whether the target matches this error.
func (e *NamespaceNotFoundError) Is(target error) bool {
	return target == ErrNamespaceNotFound
}

// ValidationError represents a validation error.
type ValidationError struct {
	Field   string
	Message string
}

// Error implements the error interface.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on %s: %s", e.Field, e.Message)
}

// IOError represents an IO error with path information.
type IOError struct {
	Operation string
	Path      string
	Cause     error
}

// Error implements the error interface.
func (e *IOError) Error() string {
	return fmt.Sprintf("IO error during %s on %s: %v", e.Operation, e.Path, e.Cause)
}

// Unwrap returns the underlying cause.
func (e *IOError) Unwrap() error {
	return e.Cause
}

// Is reports whether the target matches this error.
func (e *IOError) Is(target error) bool {
	return target == ErrIOFailed
}
