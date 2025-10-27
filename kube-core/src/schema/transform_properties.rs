use crate::schema::{InstanceType, Schema, SchemaObject, SingleOrVec};

#[cfg(test)]
#[test]
fn untagged_enum_with_empty_variant_before_one_of_hoisting() {
    let original_schema_object_value = serde_json::json!({
        "description": "An untagged enum with a nested enum inside",
        "anyOf": [
            {
                "description": "Used in case the `one` field is present",
                "type": "object",
                "required": [
                    "one"
                ],
                "properties": {
                    "one": {
                        "type": "string"
                    }
                }
            },
            {
                "description": "Used in case the `two` field is present",
                "type": "object",
                "required": [
                    "two"
                ],
                "properties": {
                    "two": {
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
                            }
                        ]
                    }
                }
            },
            {
                "description": "Used in case no fields are present",
                "type": "object"
            }
        ]
    });

    let expected_converted_schema_object_value = serde_json::json!({
        "description": "An untagged enum with a nested enum inside",
        "type": "object",
        "anyOf": [
            {
                "required": [
                    "one"
                ]
            },
            {
                "required": [
                    "two"
                ]
            },
            {}
        ],
        "properties": {
            "one": {
                "type": "string"
            },
            "two": {
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
                    }
                ]
            }
        }
    });

    let original_schema_object: SchemaObject =
        serde_json::from_value(original_schema_object_value).expect("valid JSON");
    let expected_converted_schema_object: SchemaObject =
        serde_json::from_value(expected_converted_schema_object_value).expect("valid JSON");

    let mut actual_converted_schema_object = original_schema_object.clone();
    hoist_properties_for_any_of_subschemas(&mut actual_converted_schema_object);

    assert_json_diff::assert_json_eq!(actual_converted_schema_object, expected_converted_schema_object);
}

#[cfg(test)]
#[test]
fn untagged_enum_with_duplicate_field_of_same_shape() {
    let original_schema_object_value = serde_json::json!({
        "description": "Comment for untagged enum ProductImageSelection",
        "anyOf": [
            {
                "description": "Comment for struct ProductImageCustom",
                "properties": {
                    "custom": {
                        "description": "Comment for custom field",
                        "type": "string"
                    },
                    "productVersion": {
                        "description": "Comment for product_version field (same on both structs)",
                        "type": "string"
                }
            },
                "required": [
                    "productVersion",
                    "custom"
                ],
                "type": "object"
            },
            {
                "description": "Comment for struct ProductImageVersion",
                "properties": {
                    "productVersion": {
                        "description": "Comment for product_version field (same on both structs)",
                        "type": "string"
                    },
                    "repo": {
                        "description": "Comment for repo field",
                        "nullable": true,
                        "type": "string"
                }
            },
                "required": [
                    "productVersion"
                ],
                "type": "object"
            }
        ]
    });

    let expected_converted_schema_object_value = serde_json::json!({
        "description": "Comment for untagged enum ProductImageSelection",
        "type": "object",
        "anyOf": [
            {
                "required": [
                    "custom",
                    "productVersion"
                ]
            },
            {
                "required": [
                    "productVersion"
                ]
            }
        ],
        "properties": {
            "custom": {
                "description": "Comment for custom field",
                "type": "string"
            },
            "productVersion": {
                "description": "Comment for product_version field (same on both structs)",
                "type": "string"
                    },
            "repo": {
                "description": "Comment for repo field",
                "nullable": true,
                "type": "string"
            }
        }

    });

    let original_schema_object: SchemaObject =
        serde_json::from_value(original_schema_object_value).expect("valid JSON");
    let expected_converted_schema_object: SchemaObject =
        serde_json::from_value(expected_converted_schema_object_value).expect("valid JSON");

    let mut actual_converted_schema_object = original_schema_object.clone();
    hoist_properties_for_any_of_subschemas(&mut actual_converted_schema_object);

    assert_json_diff::assert_json_eq!(actual_converted_schema_object, expected_converted_schema_object);
}

#[cfg(test)]
#[test]
#[should_panic(expected = "Properties for \"two\" are defined multiple times with different shapes")]
fn invalid_untagged_enum_with_conflicting_variant_fields_before_one_of_hosting() {
    let original_schema_object_value = serde_json::json!({
        "description": "An untagged enum with a nested enum inside",
        "anyOf": [
            {
                "description": "Used in case the `one` field is present",
                "type": "object",
                "required": [
                    "one",
                    "two"
                ],
                "properties": {
                    "one": {
                        "type": "string"
                    },
                    "two": {
                        "type": "integer"
                    }
                }
            },
            {
                "description": "Used in case the `two` field is present",
                "type": "object",
                "required": [
                    "two"
                ],
                "properties": {
                    "two": {
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
                            }
                        ]
                    }
                }
            },
            {
                "description": "Used in case no fields are present",
                "type": "object"
            }
        ]
    });


    let original_schema_object: SchemaObject =
        serde_json::from_value(original_schema_object_value).expect("valid JSON");

    let mut actual_converted_schema_object = original_schema_object.clone();
    hoist_properties_for_any_of_subschemas(&mut actual_converted_schema_object);
}

#[cfg(test)]
#[test]
#[should_panic(expected = "Properties for \"two\" are defined multiple times with different shapes")]
fn invalid_untagged_enum_with_conflicting_variant_fields_after_one_of_hosting() {
    // NOTE: the oneOf for the second variant has already been hoisted
    let original_schema_object_value = serde_json::json!({
        "description": "An untagged enum with a nested enum inside",
        "anyOf": [
            {
                "description": "Used in case the `one` field is present",
                "type": "object",
                "required": [
                    "one",
                    "two",
                ],
                "properties": {
                    "one": {
                        "type": "string"
                    },
                    "two": {
                        "type": "string"
                    }
                }
            },
            {
                "description": "Used in case the `two` field is present",
                "type": "object",
                "required": [
                    "two"
                ],
                "properties": {
                    "two": {
                        "description": "A very simple enum with unit variants",
                        "type": "string",
                        "enum": [
                            "C",
                            "D",
                            "A",
                            "B"
                        ]
                    }
                }
            },
            {
                "description": "Used in case no fields are present",
                "type": "object"
            }
        ]
    });

    let original_schema_object: SchemaObject =
        serde_json::from_value(original_schema_object_value).expect("valid JSON");

    let mut actual_converted_schema_object = original_schema_object.clone();
    hoist_properties_for_any_of_subschemas(&mut actual_converted_schema_object);
}

/// Take oneOf or anyOf subschema properties and move them them into the schema
/// properties.
///
/// Used for correcting the schema for serde untagged structural enums.
/// NOTE: Doc-comments are not preserved for untagged enums.
///
/// This will return early without modifications unless:
/// - There are `oneOf` or `anyOf` subschemas
/// - Each subschema has the type "object"
///
/// NOTE: This should work regardless of whether other hoisting has been performed or not.
pub(crate) fn hoist_properties_for_any_of_subschemas(kube_schema: &mut SchemaObject) {
    // Run some initial checks in case there is nothing to do
    let SchemaObject {
        subschemas: Some(subschemas),
        object: parent_object,
        ..
    } = kube_schema
    else {
        return;
    };

    // Deal with both tagged and untagged enums.
    // Untagged enum descriptions will not be preserved.
    let (subschemas, preserve_description) = match (subschemas.any_of.as_mut(), subschemas.one_of.as_mut()) {
        (None, None) => return,
        (None, Some(one_of)) => (one_of, true),
        (Some(any_of), None) => (any_of, false),
        (Some(_), Some(_)) => panic!("oneOf and anyOf are mutually exclusive"),
    };


    if subschemas.is_empty() {
        return;
    }

    // Ensure we aren't looking at the one with a null
    if subschemas.len() == 2 {
        // This is the signature for the null variant, indicating the "other"
        // variant is the subschema that needs hoisting
        let null = serde_json::json!({
            "enum": [null],
            "nullable": true
        });

        // Return if one of the two entries are nulls
        for value in subschemas
            .iter()
            .map(|x| serde_json::to_value(x).expect("schema should be able to convert to JSON"))
        {
            if value == null {
                return;
            }
        }
    }

    // At this point, we can be reasonably sure we need operate on the schema.
    // TODO (@NickLarsenNZ): Return errors instead of panicking, leave panicking up to the infallible schemars::Transform

    let subschemas = subschemas
        .iter_mut()
        .map(|schema| match schema {
            Schema::Object(schema_object) => schema_object,
            Schema::Bool(_) => panic!("oneOf and anyOf variants cannot be of type boolean"),
        })
        .collect::<Vec<_>>();

    for subschema in subschemas {
        // This will clear out any objects that don't have required/properties fields (so that it appears as: {}).
        let metadata = subschema.metadata.take();
        subschema.instance_type.take();

        // Set the schema type to object
        kube_schema.instance_type = Some(SingleOrVec::Single(Box::new(InstanceType::Object)));

        if let Some(object) = subschema.object.as_deref_mut() {
            // Kubernetes doesn't allow variants to set additionalProperties
            object.additional_properties.take();

            // For a tagged enum, we need to preserve the variant description
            if preserve_description {
                assert_eq!(
                    object.properties.len(),
                    1,
                    "Expecting only a single property defined for the tagged enum variant schema"
                );

                if let Schema::Object(subschema) = object
                    .properties
                    .values_mut()
                    .next()
                    .expect("asserted that one and only one property exists")
                {
                    assert!(
                        // While possible, it is unexpected if the subschema metadata is already set for the property
                        subschema.metadata.is_none(),
                        "subschema metadata for property should be empty"
                    );
                    // Move the variant description down to the properties (before they get hoisted)
                    subschema.metadata = metadata
                };
            }

            // If properties are set, hoist them to the schema properties.
            // This will panic if duplicate properties are encountered that do not have the same shape.
            // That can happen when the untagged enum variants each refer to structs which contain the same field name but with different types or doc-comments.
            // The developer needs to make them the same.
            // TODO (@NickLarsenNZ): Add a case for a structural variant, and a tuple variant containing a structure where the same field name is used.
            while let Some((property_name, Schema::Object(property_schema_object))) =
                object.properties.pop_first()
            {
                // This would check that the variant property (that we want to now hoist)
                // is exactly the same as what is already hoisted (in this function).
                if let Some(existing_property) = parent_object
                    .get_or_insert_default()
                    .properties
                    .get(&property_name)
                {
                    // TODO (@NickLarsenNZ): Here we could do another check to see if it only differs by description.
                    // If the schema property description is not set, then we could overwrite it and not panic.
                    assert_eq!(
                        existing_property,
                        &Schema::Object(property_schema_object.clone()),
                        "Properties for {property_name:?} are defined multiple times with different shapes"
                    );
                } else {
                    // Otherwise, insert the subschema properties into the schema properties
                    parent_object
                        .get_or_insert_default()
                        .properties
                        .insert(property_name.clone(), Schema::Object(property_schema_object));
                }
            }
        }
    }
}
