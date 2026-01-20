package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-iceberg/go-iceberg/spec"
)

// RESTCatalog implements the Iceberg REST Catalog API.
type RESTCatalog struct {
	name       string
	uri        string
	warehouse  string
	client     *http.Client
	token      string
	credential string
	properties map[string]string
}

// RESTCatalogOption configures a REST catalog.
type RESTCatalogOption func(*RESTCatalog)

// WithName sets the catalog name.
func WithName(name string) RESTCatalogOption {
	return func(c *RESTCatalog) {
		c.name = name
	}
}

// WithWarehouse sets the warehouse location.
func WithWarehouse(warehouse string) RESTCatalogOption {
	return func(c *RESTCatalog) {
		c.warehouse = warehouse
	}
}

// WithToken sets the bearer token for authentication.
func WithToken(token string) RESTCatalogOption {
	return func(c *RESTCatalog) {
		c.token = token
	}
}

// WithCredential sets the credential for OAuth2 authentication.
func WithCredential(credential string) RESTCatalogOption {
	return func(c *RESTCatalog) {
		c.credential = credential
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) RESTCatalogOption {
	return func(c *RESTCatalog) {
		c.client = client
	}
}

// WithRESTProperties sets additional properties.
func WithRESTProperties(props map[string]string) RESTCatalogOption {
	return func(c *RESTCatalog) {
		for k, v := range props {
			c.properties[k] = v
		}
	}
}

// NewRESTCatalog creates a new REST catalog client.
func NewRESTCatalog(uri string, opts ...RESTCatalogOption) (*RESTCatalog, error) {
	c := &RESTCatalog{
		name:       "rest",
		uri:        strings.TrimSuffix(uri, "/"),
		client:     &http.Client{Timeout: 30 * time.Second},
		properties: make(map[string]string),
	}

	for _, opt := range opts {
		opt(c)
	}

	return c, nil
}

// Name returns the catalog name.
func (c *RESTCatalog) Name() string {
	return c.name
}

// doRequest executes an HTTP request.
func (c *RESTCatalog) doRequest(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.uri+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	if c.warehouse != "" {
		q := req.URL.Query()
		q.Set("warehouse", c.warehouse)
		req.URL.RawQuery = q.Encode()
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

// parseResponse parses an HTTP response.
func parseResponse[T any](resp *http.Response, v *T) error {
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Error struct {
				Message string `json:"message"`
				Type    string `json:"type"`
				Code    int    `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(body, &errResp) == nil && errResp.Error.Message != "" {
			return fmt.Errorf("REST API error: %s (code: %d, type: %s)",
				errResp.Error.Message, errResp.Error.Code, errResp.Error.Type)
		}
		return fmt.Errorf("REST API error: status %d: %s", resp.StatusCode, string(body))
	}

	if v != nil && len(body) > 0 {
		if err := json.Unmarshal(body, v); err != nil {
			return fmt.Errorf("failed to unmarshal response: %w", err)
		}
	}

	return nil
}

// namespacePath returns the API path for a namespace.
func namespacePath(ns Namespace) string {
	return "/v1/namespaces/" + url.PathEscape(ns.String())
}

// tablePath returns the API path for a table.
func tablePath(id TableIdentifier) string {
	return namespacePath(id.Namespace) + "/tables/" + url.PathEscape(id.Name)
}

// ListNamespaces lists all namespaces.
func (c *RESTCatalog) ListNamespaces(ctx context.Context, parent Namespace) ([]Namespace, error) {
	path := "/v1/namespaces"
	if len(parent) > 0 {
		path += "?parent=" + url.QueryEscape(parent.String())
	}

	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Namespaces [][]string `json:"namespaces"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	namespaces := make([]Namespace, len(result.Namespaces))
	for i, ns := range result.Namespaces {
		namespaces[i] = Namespace(ns)
	}

	return namespaces, nil
}

// CreateNamespace creates a new namespace.
func (c *RESTCatalog) CreateNamespace(ctx context.Context, namespace Namespace, properties map[string]string) error {
	body := map[string]any{
		"namespace":  namespace,
		"properties": properties,
	}

	resp, err := c.doRequest(ctx, http.MethodPost, "/v1/namespaces", body)
	if err != nil {
		return err
	}

	return parseResponse(resp, (*any)(nil))
}

// DropNamespace drops a namespace.
func (c *RESTCatalog) DropNamespace(ctx context.Context, namespace Namespace) error {
	resp, err := c.doRequest(ctx, http.MethodDelete, namespacePath(namespace), nil)
	if err != nil {
		return err
	}

	return parseResponse(resp, (*any)(nil))
}

// NamespaceExists checks if a namespace exists.
func (c *RESTCatalog) NamespaceExists(ctx context.Context, namespace Namespace) (bool, error) {
	resp, err := c.doRequest(ctx, http.MethodHead, namespacePath(namespace), nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 400 {
		return false, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return true, nil
}

// LoadNamespaceProperties loads namespace properties.
func (c *RESTCatalog) LoadNamespaceProperties(ctx context.Context, namespace Namespace) (map[string]string, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, namespacePath(namespace), nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Properties, nil
}

// UpdateNamespaceProperties updates namespace properties.
func (c *RESTCatalog) UpdateNamespaceProperties(ctx context.Context, namespace Namespace, removals []string, updates map[string]string) error {
	body := map[string]any{
		"removals": removals,
		"updates":  updates,
	}

	resp, err := c.doRequest(ctx, http.MethodPost, namespacePath(namespace)+"/properties", body)
	if err != nil {
		return err
	}

	return parseResponse(resp, (*any)(nil))
}

// ListTables lists all tables in a namespace.
func (c *RESTCatalog) ListTables(ctx context.Context, namespace Namespace) ([]TableIdentifier, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, namespacePath(namespace)+"/tables", nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	tables := make([]TableIdentifier, len(result.Identifiers))
	for i, id := range result.Identifiers {
		tables[i] = TableIdentifier{
			Namespace: Namespace(id.Namespace),
			Name:      id.Name,
		}
	}

	return tables, nil
}

// CreateTable creates a new table.
func (c *RESTCatalog) CreateTable(ctx context.Context, identifier TableIdentifier, schema *spec.Schema, opts ...CreateTableOption) (*spec.TableMetadata, error) {
	cfg := &CreateTableConfig{
		Properties: make(map[string]string),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	body := map[string]any{
		"name":   identifier.Name,
		"schema": schema,
	}

	if cfg.PartitionSpec != nil {
		body["partition-spec"] = cfg.PartitionSpec
	}
	if cfg.SortOrder != nil {
		body["write-order"] = cfg.SortOrder
	}
	if cfg.Location != "" {
		body["location"] = cfg.Location
	}
	if len(cfg.Properties) > 0 {
		body["properties"] = cfg.Properties
	}
	if cfg.StageCreate {
		body["stage-create"] = true
	}

	resp, err := c.doRequest(ctx, http.MethodPost, namespacePath(identifier.Namespace)+"/tables", body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Metadata *spec.TableMetadata `json:"metadata"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Metadata, nil
}

// LoadTable loads a table's metadata.
func (c *RESTCatalog) LoadTable(ctx context.Context, identifier TableIdentifier) (*spec.TableMetadata, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, tablePath(identifier), nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Metadata     *spec.TableMetadata `json:"metadata"`
		MetadataLocation string          `json:"metadata-location"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Metadata, nil
}

// TableExists checks if a table exists.
func (c *RESTCatalog) TableExists(ctx context.Context, identifier TableIdentifier) (bool, error) {
	resp, err := c.doRequest(ctx, http.MethodHead, tablePath(identifier), nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 400 {
		return false, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	return true, nil
}

// DropTable drops a table.
func (c *RESTCatalog) DropTable(ctx context.Context, identifier TableIdentifier, purge bool) error {
	path := tablePath(identifier)
	if purge {
		path += "?purgeRequested=true"
	}

	resp, err := c.doRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return err
	}

	return parseResponse(resp, (*any)(nil))
}

// RenameTable renames a table.
func (c *RESTCatalog) RenameTable(ctx context.Context, from, to TableIdentifier) error {
	body := map[string]any{
		"source": map[string]any{
			"namespace": from.Namespace,
			"name":      from.Name,
		},
		"destination": map[string]any{
			"namespace": to.Namespace,
			"name":      to.Name,
		},
	}

	resp, err := c.doRequest(ctx, http.MethodPost, "/v1/tables/rename", body)
	if err != nil {
		return err
	}

	return parseResponse(resp, (*any)(nil))
}

// CommitTable commits changes to a table.
func (c *RESTCatalog) CommitTable(ctx context.Context, identifier TableIdentifier, requirements []TableRequirement, updates []TableUpdate) (*spec.TableMetadata, error) {
	body := map[string]any{
		"requirements": requirements,
		"updates":      updates,
	}

	resp, err := c.doRequest(ctx, http.MethodPost, tablePath(identifier), body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Metadata         *spec.TableMetadata `json:"metadata"`
		MetadataLocation string              `json:"metadata-location"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	return result.Metadata, nil
}

// OAuth2TokenResponse represents an OAuth2 token response.
type OAuth2TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	IssuedTokenType string `json:"issued_token_type"`
}

// FetchToken fetches an OAuth2 token from the catalog.
func (c *RESTCatalog) FetchToken(ctx context.Context, credential string) (*OAuth2TokenResponse, error) {
	body := "grant_type=client_credentials&client_id=" + url.QueryEscape(credential)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.uri+"/v1/oauth/tokens", strings.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	var token OAuth2TokenResponse
	if err := parseResponse(resp, &token); err != nil {
		return nil, err
	}

	return &token, nil
}

// Config retrieves catalog configuration.
func (c *RESTCatalog) Config(ctx context.Context) (map[string]string, error) {
	path := "/v1/config"
	if c.warehouse != "" {
		path += "?warehouse=" + url.QueryEscape(c.warehouse)
	}

	resp, err := c.doRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	var result struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	if err := parseResponse(resp, &result); err != nil {
		return nil, err
	}

	// Merge defaults and overrides
	config := make(map[string]string)
	for k, v := range result.Defaults {
		config[k] = v
	}
	for k, v := range result.Overrides {
		config[k] = v
	}

	return config, nil
}
