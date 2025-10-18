package bsonvalidator

import (
	"errors"
	"reflect"
	"strconv"
)

type GlowStickCollection struct {
	Name [32]byte `bson:"name" required:"true maxlen:32 type:string"`
	Mode [7]byte  `bson:"mode" required:"false maxlen:7 type:string"`
}

// ValidateBson validates a map[string]interface{} (i.e. BSON doc) against the UserSchema.
func ValidateBson(doc map[string]interface{}) error {
	schema := reflect.TypeOf(GlowStickCollection{})
	for i := 0; i < schema.NumField(); i++ {
		field := schema.Field(i)
		key := field.Tag.Get("bson")
		required := field.Tag.Get("required") == "true"
		maxlenTag := field.Tag.Get("maxlen")
		var maxlen int
		if maxlenTag != "" {
			var err error
			maxlen, err = strconv.Atoi(maxlenTag)
			if err != nil {
				return errors.New("invalid maxlen tag value for field: " + key)
			}
		}

		val, exists := doc[key]
		if required && !exists {
			return errors.New("missing required field: " + key)
		}
		if exists {
			if reflect.TypeOf(val).Kind() != reflect.String {
				return errors.New("field " + key + " must be a string")
			}
			if len(val.(string)) > maxlen {

			}
		}
	}
	return nil
}
