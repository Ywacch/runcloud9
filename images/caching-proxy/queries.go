package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/ywacch/cloud9/pkg/templateapp/readrouter"
	"gopkg.in/yaml.v3"
)

// YAML matches pkg/templates.ProxyQueryRule (same file shape as Deployer-marshaled queries.yaml).

type proxyParamSpec struct {
	Capture int    `yaml:"capture"`
	DBType  string `yaml:"dbType"`
}

type proxyQueryRuleYAML struct {
	Pattern string           `yaml:"pattern"`
	SQL     string           `yaml:"sql"`
	Params  []proxyParamSpec `yaml:"params"`
}

type compiledProxyRule struct {
	re     *regexp.Regexp
	sql    string
	params []proxyParamSpec
}

var errNoMatchingProxyRule = errors.New("no matching proxyQueries rule")

func mustLoadCompiledProxyRules(path string) []compiledProxyRule {
	data, err := os.ReadFile(path)
	if err != nil {
		slog.Error("missing proxy query manifest", "path", path, "error", err)
		os.Exit(1)
	}
	var defs []proxyQueryRuleYAML
	if err := yaml.Unmarshal(data, &defs); err != nil {
		slog.Error("invalid proxyQueries yaml", "path", path, "error", err)
		os.Exit(1)
	}
	if len(defs) == 0 {
		slog.Error("proxyQueries yaml is empty", "path", path)
		os.Exit(1)
	}
	out := make([]compiledProxyRule, 0, len(defs))
	for _, d := range defs {
		re, err := regexp.Compile(d.Pattern)
		if err != nil {
			slog.Error("bad proxyQueries pattern", "pattern", d.Pattern, "error", err)
			os.Exit(1)
		}
		sqlText := strings.TrimSpace(d.SQL)
		if sqlText == "" {
			slog.Error("proxyQueries rule missing sql", "pattern", d.Pattern)
			os.Exit(1)
		}
		out = append(out, compiledProxyRule{re: re, sql: sqlText, params: d.Params})
	}
	return out
}

func captureArgs(matches []string, params []proxyParamSpec) ([]any, error) {
	args := make([]any, 0, len(params))
	for _, p := range params {
		idx := p.Capture
		if idx <= 0 || idx >= len(matches) {
			return nil, fmt.Errorf("invalid capture index %d", idx)
		}
		raw := strings.TrimSpace(matches[idx])
		switch strings.ToLower(p.DBType) {
		case "text", "":
			args = append(args, raw)
		case "integer":
			n, err := strconv.Atoi(raw)
			if err != nil {
				return nil, fmt.Errorf("integer param %q: %w", raw, err)
			}
			args = append(args, n)
		case "boolean":
			b, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, fmt.Errorf("boolean param %q: %w", raw, err)
			}
			args = append(args, b)
		default:
			return nil, fmt.Errorf("unsupported dbType %q", p.DBType)
		}
	}
	return args, nil
}

func proxyReadThrough(ctx context.Context, q readrouter.Querier, rules []compiledProxyRule, cacheKey string) (string, error) {
	for _, rule := range rules {
		matches := rule.re.FindStringSubmatch(cacheKey)
		if matches == nil {
			continue
		}
		args, err := captureArgs(matches, rule.params)
		if err != nil {
			return "", fmt.Errorf("%w: %v", errBadCapture, err)
		}
		rows, err := q.Query(ctx, rule.sql, args...)
		if err != nil {
			return "", err
		}
		payload, convErr := rowsToProxyValue(rows)
		if convErr != nil {
			return "", convErr
		}
		return payload, nil
	}
	return "", errNoMatchingProxyRule
}

var errBadCapture = errors.New("bad capture")

func rowsToProxyValue(rows pgx.Rows) (string, error) {
	defer rows.Close()
	fd := rows.FieldDescriptions()
	if len(fd) == 0 {
		return "", errors.New("no columns returned")
	}
	colNames := make([]string, len(fd))
	for i := range fd {
		colNames[i] = fd[i].Name
	}

	var collected [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return "", err
		}
		collected = append(collected, vals)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(collected) == 0 {
		return "", pgx.ErrNoRows
	}

	numCols := len(fd)
	if numCols == 1 && len(collected) == 1 {
		return stringifyPayloadCell(collected[0][0]), nil
	}
	if numCols == 1 {
		arr := make([]any, len(collected))
		for i, row := range collected {
			arr[i] = jsonCleanValue(row[0])
		}
		b, err := json.Marshal(arr)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	outMaps := make([]map[string]any, len(collected))
	for ri, vals := range collected {
		m := make(map[string]any, len(colNames))
		for i, name := range colNames {
			m[name] = jsonCleanValue(vals[i])
		}
		outMaps[ri] = m
	}
	if len(outMaps) == 1 {
		b, err := json.Marshal(outMaps[0])
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
	b, err := json.Marshal(outMaps)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func stringifyPayloadCell(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return string(mustMarshalJSON(jsonCleanValue(v)))
	}
}

func jsonCleanValue(v any) any {
	switch x := v.(type) {
	case []byte:
		if len(x) > 0 && (x[0] == '{' || x[0] == '[') && json.Valid(x) {
			return json.RawMessage(x)
		}
		return string(x)
	default:
		return x
	}
}

func mustMarshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
