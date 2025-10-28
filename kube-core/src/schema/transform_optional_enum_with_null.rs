use serde_json::Value;

use super::SchemaObject;

/// A Option<Enum> without any descriptions has `nullable` set to `true` as well as a trailing
/// `null` enum variant. We remove the trailing enum variant as it's not needed for Kubernetes and
/// makes the CRD more compact by removing duplicated information.
pub(crate) fn remove_optional_enum_null_variant(kube_schema: &mut SchemaObject) {
    let SchemaObject {
        enum_values: Some(enum_values),
        extensions,
        ..
    } = kube_schema
    else {
        return;
    };

    // It only makes sense to remove `null` enum values in case the enum is
    // nullable (thus optional).
    if let Some(Value::Bool(true)) = extensions.get("nullable") {
        // Don't remove the single last enum variant. This often happens for
        // `Option<XXX>`, which is represented as
        // `"anyOf": [XXX, {"enum": [null], "optional": true}]`
        if enum_values.len() > 1 {
            enum_values.retain(|enum_value| enum_value != &Value::Null);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_enum_with_null() {
        let original_schema_object_value = serde_json::json!({
            "description": "A very simple enum with unit variants without descriptions",
            "enum": [
                "A",
                "B",
                "C",
                "D",
                null
            ],
            "nullable": true
        });

        let expected_converted_schema_object_value = serde_json::json!({
            "description": "A very simple enum with unit variants without descriptions",
            "enum": [
                "A",
                "B",
                "C",
                "D"
            ],
            "nullable": true
        });

        let original_schema_object: SchemaObject =
            serde_json::from_value(original_schema_object_value).expect("valid JSON");
        let expected_converted_schema_object: SchemaObject =
            serde_json::from_value(expected_converted_schema_object_value).expect("valid JSON");

        let mut actual_converted_schema_object = original_schema_object.clone();
        remove_optional_enum_null_variant(&mut actual_converted_schema_object);

        assert_json_diff::assert_json_eq!(actual_converted_schema_object, expected_converted_schema_object);
    }
}
