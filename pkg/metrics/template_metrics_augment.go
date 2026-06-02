package metrics

import (
	"reflect"
	"strings"

	"github.com/ywacch/cloud9/pkg/templates"
)

func applyPrometheusMetricAugment(dm *DeploymentMetrics, def templates.PrometheusMetricAugmentDef) {
	cm, ok := dm.Components[def.Component]
	if !ok {
		return
	}
	v, ok := resolveAugmentPath(cm, def.Path)
	if !ok {
		return
	}
	if def.OnlyIfPositive && v <= 0 {
		return
	}
	dm.TemplateMetrics[def.Key] = v
}

// resolveAugmentPath resolves a dot path (e.g. storageIo.readRate) against ComponentMetrics.
func resolveAugmentPath(cm ComponentMetrics, path string) (float64, bool) {
	if path == "" {
		return 0, false
	}
	val := reflect.ValueOf(cm)
	for _, part := range strings.Split(path, ".") {
		if !val.IsValid() {
			return 0, false
		}
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return 0, false
			}
			val = val.Elem()
		}
		if val.Kind() != reflect.Struct {
			return 0, false
		}
		next, ok := structFieldByPathPart(val, part)
		if !ok {
			return 0, false
		}
		val = next
	}

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return 0, false
		}
		val = val.Elem()
	}
	switch val.Kind() {
	case reflect.Float32, reflect.Float64:
		return val.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint()), true
	default:
		return 0, false
	}
}

func structFieldByPathPart(v reflect.Value, part string) (reflect.Value, bool) {
	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		jsonTag := sf.Tag.Get("json")
		if idx := strings.Index(jsonTag, ","); idx >= 0 {
			jsonTag = jsonTag[:idx]
		}
		if strings.EqualFold(sf.Name, part) || strings.EqualFold(jsonTag, part) {
			return v.Field(i), true
		}
	}
	return reflect.Value{}, false
}
