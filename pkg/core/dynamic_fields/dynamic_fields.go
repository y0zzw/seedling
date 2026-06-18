package dynamicfields

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	. "v-gnms/pkg/core/filesystem"

	"github.com/tidwall/gjson"
)

func ConstructDynamicDeviceFieldsFromYAMLData(yamlContentInJson string) map[string]interface{} {
	var result = make(map[string]interface{})

	dataFields := gjson.Get(yamlContentInJson, "data_fields")

	dataFields.ForEach(func(key, val gjson.Result) bool {
		requiredDataType := val.Get("type").String()
		switch requiredDataType {
		case "float":
			result[key.String()] = 0.0
		case "int":
			result[key.String()] = 0
		case "string":
			result[key.String()] = ""
		case "bool":
			result[key.String()] = false
		case "array<float>":
			result[key.String()] = []float64{}
		case "array<int>":
			result[key.String()] = []int{}
		case "array<string>":
			result[key.String()] = []string{}
		case "array<bool>":
			result[key.String()] = []bool{}
		}
		return true
	})

	return result
}

func ExtractAndValidateDynamicData(yamlDefinitionInJson string, existingDataFields map[string]interface{}, newDataFields map[string]interface{}) (map[string]interface{}, error) {
	for key, val := range newDataFields {
		rule := gjson.Get(yamlDefinitionInJson, fmt.Sprintf("data_fields.%s", key))
		err := validateDynamicData(&rule, val)
		if err != nil {
			return nil, err
		}

		existingDataFields[key] = newDataFields[key]
	}

	return existingDataFields, nil
}

func ExtractAndValidateQueryDownlinkPayload(queryTarget string, payload map[string]interface{}, deviceId string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})

	yamlContentInJson, err := GetDeviceDefinitionYAMLAsJSONFromDeviceID(deviceId)
	if err != nil {
		return nil, err
	}

	// commandDef := gjson.Get(yamlContentInJson, fmt.Sprintf("commands"))
	downlinkFieldsDef := gjson.Get(yamlContentInJson, fmt.Sprintf("query.%s.downlink_fields", queryTarget))
	if !downlinkFieldsDef.Exists() {
		return nil, fmt.Errorf("query target '%s' not found in device definition", queryTarget)
	}

	for key, rule := range downlinkFieldsDef.Map() {
		val, ok := payload[key]

		// If the key is not found in the payload
		if !ok {
			// Error, if the key is required
			// Otherwise, skip
			required := rule.Get("required")
			if required.Exists() && required.Bool() {
				return nil, fmt.Errorf("missing payload value '%s'", key)
			} else {
				continue
			}
		}

		err := validateDynamicData(&rule, val)
		if err != nil {
			return nil, err
		}

		result[key] = val
	}

	return result, err
}

func ExtractAndValidateCommandDownlinkPayload(commandName string, payload map[string]interface{}, deviceId string) (map[string]interface{}, error) {
	var result = make(map[string]interface{})

	yamlContentInJson, err := GetDeviceDefinitionYAMLAsJSONFromDeviceID(deviceId)
	if err != nil {
		return nil, err
	}

	// commandDef := gjson.Get(yamlContentInJson, fmt.Sprintf("commands"))
	downlinkFieldsDef := gjson.Get(yamlContentInJson, fmt.Sprintf("commands.%s.downlink_fields", commandName))
	if !downlinkFieldsDef.Exists() {
		return nil, fmt.Errorf("command '%s' not found in device definition", commandName)
	}

	for key, rule := range downlinkFieldsDef.Map() {
		val, ok := payload[key]

		// If the key is not found in the payload
		if !ok {
			// Error, if the key is required
			// Otherwise, skip
			required := rule.Get("required")
			if required.Exists() && required.Bool() {
				return nil, fmt.Errorf("missing payload value '%s'", key)
			} else {
				continue
			}
		}

		err := validateDynamicData(&rule, val)
		if err != nil {
			return nil, err
		}

		result[key] = val
	}

	return result, err
}

func validateDynamicData(rule *gjson.Result, value any) error {
	requiredDataType := rule.Get("type")
	min := rule.Get("min")
	max := rule.Get("max")
	maxLength := rule.Get("maxlength")

	isDataTypeCorrect := true

	actualType := reflect.TypeOf(value).String()
	actualType = strings.ReplaceAll(actualType, "64", "") // Convert "float64" to "float"

	if actualType != requiredDataType.String() {
		isDataTypeCorrect = false
	}

	// If the floored/ceiled value is equal to the raw value, it means the value is an interger
	// This is a fix for wrong determination of int to float64
	if actualType == "float" {
		if math.Floor(value.(float64)) == value.(float64) || math.Ceil(value.(float64)) == value.(float64) {
			isDataTypeCorrect = true
		}
	}

	// "[]interface {}" is equivalent to "array<any>"
	if strings.Contains(requiredDataType.Str, "array") || actualType == "[]interface {}" {
		isDataTypeCorrect = true
	}

	// log.Info().Any("actualType", actualType).Msg("")
	// log.Info().Any("requiredDataType", requiredDataType.Str).Msg("")

	if !isDataTypeCorrect {
		return errors.New("dynamic data validation failed")
	}

	validationPassed := true

	switch requiredDataType.String() {
	case "float":
		validationPassed = checkNumericMinMax(&min, &max, &value)
	case "int":
		validationPassed = checkNumericMinMax(&min, &max, &value)
	case "string":
		validationPassed = checkMaxLength(&maxLength, &value)
	}

	if !validationPassed {
		return errors.New("dynamic data validation failed")
	}

	return nil
}

func checkNumericMinMax(min *gjson.Result, max *gjson.Result, value *any) bool {
	val := reflect.ValueOf(*value).Float()

	if min.Exists() && val < min.Num {
		return false
	}

	if max.Exists() && val > max.Num {
		return false
	}

	return true
}

func checkMaxLength(maxLength *gjson.Result, value *any) bool {
	if maxLength.Exists() && len((*value).(string)) > int(maxLength.Num) {
		return false
	}
	return true
}
