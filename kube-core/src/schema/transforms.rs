use std::ops::DerefMut;

use crate::schema::{Schema, SchemaObject, SubschemaValidation};

#[cfg(test)]
#[test]
fn tagged_enum_with_unit_variants() {
    let original_schema_object_value = serde_json::json!({
        "description": "A very simple enum with unit variants",
        "oneOf": [
            {
                "type": "string",
                "enum": [
                    "C",
                    "D"
                ]
            },
            {
                "description": "First variant doc-comment",
                "type": "string",
                "enum": [
                    "A"
                ]
            },
            {
                "description": "Second variant doc-comment",
                "type": "string",
                "enum": [
                    "B"
                ]
            },
        ]
    });

    let expected_converted_schema_object_value = serde_json::json!({
        "description": "A very simple enum with unit variants",
        "type": "string",
        "enum": [
            "C",
            "D",
            "A",
            "B"
        ]
    });


    let original_schema_object: SchemaObject =
        serde_json::from_value(original_schema_object_value).expect("valid JSON");
    let expected_converted_schema_object: SchemaObject =
        serde_json::from_value(expected_converted_schema_object_value).expect("valid JSON");

    let mut actual_converted_schema_object = original_schema_object.clone();
    hoist_one_of_enum_with_unit_variants(&mut actual_converted_schema_object);

    assert_json_diff::assert_json_eq!(actual_converted_schema_object, expected_converted_schema_object);
}


/// Replace a list of typed oneOf subschemas with a typed schema level enum
///
/// Used for correcting the schema for tagged enums with unit variants.
/// NOTE: Subschema descriptions are lost when they are combined into a single enum of the same type.
///
/// This will return early without modifications unless:
/// - There are `oneOf` subschemas (not empty)
/// - Each subschema contains an enum
/// - Each subschema is typed
/// - Each subschemas types is the same as the others
///
/// NOTE: This should work regardless of whether other hoisting has been performed or not.
fn hoist_one_of_enum_with_unit_variants(kube_schema: &mut SchemaObject) {
    // Run some initial checks in case there is nothing to do
    let SchemaObject {
        subschemas: Some(subschemas),
        ..
    } = kube_schema
    else {
        return;
    };

    let SubschemaValidation {
        one_of: Some(one_of), ..
    } = subschemas.deref_mut()
    else {
        return;
    };

    if one_of.is_empty() {
        return;
    }

    // At this point, we can be reasonably sure we need to hoist the oneOf
    // subschema enums and types up to the schema level, and unset the oneOf field.
    // From here, anything that looks wrong will panic instead of return.
    // TODO (@NickLarsenNZ): Return errors instead of panicking, leave panicking up to the infallible schemars::Transform

    // Prepare to ensure each variant schema has a type
    let mut types = one_of.iter().map(|schema| match schema {
        Schema::Object(SchemaObject {
            instance_type: Some(r#type),
            ..
        }) => r#type,
        Schema::Object(untyped) => panic!("oneOf variants need to define a type: {untyped:#?}"),
        Schema::Bool(_) => panic!("oneOf variants can not be of type boolean"),
    });

    // Get the first type
    let variant_type = types.next().expect("at this point, there must be a type");
    // Ensure all variant types match it
    if types.any(|r#type| r#type != variant_type) {
        panic!("oneOf variants must all have the same type");
    }

    // For each `oneOf` entry, iterate over the `enum` and `const` values.
    // Panic on an entry that doesn't contain an `enum` or `const`.
    let new_enums = one_of.iter().flat_map(|schema| match schema {
        Schema::Object(SchemaObject {
            enum_values: Some(r#enum),
            ..
        }) => r#enum.clone(),
        // Warning: The `const` check below must come after the enum check above.
        // Otherwise it will panic on a valid entry with an `enum`.
        Schema::Object(SchemaObject { other, .. }) => match other.get("const") {
            Some(r#const) => vec![r#const.clone()],
            None => panic!("oneOf variant did not provide \"enum\" or \"const\": {schema:#?}"),
        },
        Schema::Bool(_) => panic!("oneOf variants can not be of type boolean"),
    });
    // Merge the enums (extend just to be safe)
    kube_schema.enum_values.get_or_insert_default().extend(new_enums);

    // Hoist the type
    kube_schema.instance_type = Some(variant_type.clone());

    // Clear the oneOf subschemas
    subschemas.one_of = None;
}
