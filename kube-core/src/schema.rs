//! Utilities for managing [`CustomResourceDefinition`] schemas
//!
//! [`CustomResourceDefinition`]: `k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition`

// Used in docs
#[allow(unused_imports)] use schemars::generate::SchemaSettings;

use schemars::{transform::Transform, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{btree_map::Entry, BTreeMap, BTreeSet},
    ops::Deref as _,
};

/// schemars [`Visitor`] that rewrites a [`Schema`] to conform to Kubernetes' "structural schema" rules
///
/// The following two transformations are applied
///  * Rewrite enums from `oneOf` to `object`s with multiple variants ([schemars#84](https://github.com/GREsau/schemars/issues/84))
///  * Rewrite untagged enums from `anyOf` to `object`s with multiple variants ([kube#1028](https://github.com/kube-rs/kube/pull/1028))
///  * Rewrite `additionalProperties` from `#[serde(flatten)]` to `x-kubernetes-preserve-unknown-fields` ([kube#844](https://github.com/kube-rs/kube/issues/844))
///
/// This is used automatically by `kube::derive`'s `#[derive(CustomResource)]`,
/// but it can also be used manually with [`SchemaSettings::with_transform`].
///
/// # Panics
///
/// The [`Visitor`] functions may panic if the transform could not be applied. For example,
/// there must not be any overlapping properties between `oneOf` branches.
#[derive(Debug, Clone)]
pub struct StructuralSchemaRewriter;

/// A JSON Schema.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, JsonSchema)]
#[serde(untagged)]
enum Schema {
    /// A trivial boolean JSON Schema.
    ///
    /// The schema `true` matches everything (always passes validation), whereas the schema `false`
    /// matches nothing (always fails validation).
    Bool(bool),
    /// A JSON Schema object.
    Object(SchemaObject),
}

/// A JSON Schema object.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct SchemaObject {
    /// Properties which annotate the [`SchemaObject`] which typically have no effect when an object is being validated against the schema.
    #[serde(flatten, deserialize_with = "skip_if_default")]
    metadata: Option<Box<Metadata>>,
    /// The `type` keyword.
    ///
    /// See [JSON Schema Validation 6.1.1. "type"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.1.1)
    /// and [JSON Schema 4.2.1. Instance Data Model](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-4.2.1).
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    instance_type: Option<SingleOrVec<InstanceType>>,
    /// The `format` keyword.
    ///
    /// See [JSON Schema Validation 7. A Vocabulary for Semantic Content With "format"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-7).
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<String>,
    /// The `enum` keyword.
    ///
    /// See [JSON Schema Validation 6.1.2. "enum"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.1.2)
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    enum_values: Option<Vec<Value>>,
    /// Properties of the [`SchemaObject`] which define validation assertions in terms of other schemas.
    #[serde(flatten, deserialize_with = "skip_if_default")]
    subschemas: Option<Box<SubschemaValidation>>,
    /// Properties of the [`SchemaObject`] which define validation assertions for arrays.
    #[serde(flatten, deserialize_with = "skip_if_default")]
    array: Option<Box<ArrayValidation>>,
    /// Properties of the [`SchemaObject`] which define validation assertions for objects.
    #[serde(flatten, deserialize_with = "skip_if_default")]
    object: Option<Box<ObjectValidation>>,
    /// Arbitrary extra properties which are not part of the JSON Schema specification, or which `schemars` does not support.
    #[serde(flatten)]
    extensions: BTreeMap<String, Value>,
    /// Arbitrary data.
    #[serde(flatten)]
    other: Value,
}

// Deserializing "null" to `Option<Value>` directly results in `None`,
// this function instead makes it deserialize to `Some(Value::Null)`.
fn allow_null<'de, D>(de: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Value::deserialize(de).map(Option::Some)
}

fn skip_if_default<'de, D, T>(deserializer: D) -> Result<Option<Box<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de> + Default + PartialEq,
{
    let value = T::deserialize(deserializer)?;
    if value == T::default() {
        Ok(None)
    } else {
        Ok(Some(Box::new(value)))
    }
}

/// Properties which annotate a [`SchemaObject`] which typically have no effect when an object is being validated against the schema.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct Metadata {
    /// The `description` keyword.
    ///
    /// See [JSON Schema Validation 9.1. "title" and "description"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-9.1).
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// The `default` keyword.
    ///
    /// See [JSON Schema Validation 9.2. "default"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-9.2).
    #[serde(skip_serializing_if = "Option::is_none", deserialize_with = "allow_null")]
    default: Option<Value>,
}

/// Properties of a [`SchemaObject`] which define validation assertions in terms of other schemas.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct SubschemaValidation {
    /// The `anyOf` keyword.
    ///
    /// See [JSON Schema 9.2.1.2. "anyOf"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.2.1.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    any_of: Option<Vec<Schema>>,
    /// The `oneOf` keyword.
    ///
    /// See [JSON Schema 9.2.1.3. "oneOf"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.2.1.3).
    #[serde(skip_serializing_if = "Option::is_none")]
    one_of: Option<Vec<Schema>>,
}

/// Properties of a [`SchemaObject`] which define validation assertions for arrays.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct ArrayValidation {
    /// The `items` keyword.
    ///
    /// See [JSON Schema 9.3.1.1. "items"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.1.1).
    #[serde(skip_serializing_if = "Option::is_none")]
    items: Option<SingleOrVec<Schema>>,
    /// The `additionalItems` keyword.
    ///
    /// See [JSON Schema 9.3.1.2. "additionalItems"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.1.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_items: Option<Box<Schema>>,
    /// The `maxItems` keyword.
    ///
    /// See [JSON Schema Validation 6.4.1. "maxItems"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.4.1).
    #[serde(skip_serializing_if = "Option::is_none")]
    max_items: Option<u32>,
    /// The `minItems` keyword.
    ///
    /// See [JSON Schema Validation 6.4.2. "minItems"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.4.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    min_items: Option<u32>,
    /// The `uniqueItems` keyword.
    ///
    /// See [JSON Schema Validation 6.4.3. "uniqueItems"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.4.3).
    #[serde(skip_serializing_if = "Option::is_none")]
    unique_items: Option<bool>,
    /// The `contains` keyword.
    ///
    /// See [JSON Schema 9.3.1.4. "contains"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.1.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    contains: Option<Box<Schema>>,
}

/// Properties of a [`SchemaObject`] which define validation assertions for objects.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default, JsonSchema)]
#[serde(rename_all = "camelCase", default)]
struct ObjectValidation {
    /// The `maxProperties` keyword.
    ///
    /// See [JSON Schema Validation 6.5.1. "maxProperties"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.5.1).
    #[serde(skip_serializing_if = "Option::is_none")]
    max_properties: Option<u32>,
    /// The `minProperties` keyword.
    ///
    /// See [JSON Schema Validation 6.5.2. "minProperties"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.5.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    min_properties: Option<u32>,
    /// The `required` keyword.
    ///
    /// See [JSON Schema Validation 6.5.3. "required"](https://tools.ietf.org/html/draft-handrews-json-schema-validation-02#section-6.5.3).
    #[serde(skip_serializing_if = "BTreeSet::is_empty")]
    required: BTreeSet<String>,
    /// The `properties` keyword.
    ///
    /// See [JSON Schema 9.3.2.1. "properties"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.2.1).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    properties: BTreeMap<String, Schema>,
    /// The `patternProperties` keyword.
    ///
    /// See [JSON Schema 9.3.2.2. "patternProperties"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.2.2).
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pattern_properties: BTreeMap<String, Schema>,
    /// The `additionalProperties` keyword.
    ///
    /// See [JSON Schema 9.3.2.3. "additionalProperties"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.2.3).
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_properties: Option<Box<Schema>>,
    /// The `propertyNames` keyword.
    ///
    /// See [JSON Schema 9.3.2.5. "propertyNames"](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-9.3.2.5).
    #[serde(skip_serializing_if = "Option::is_none")]
    property_names: Option<Box<Schema>>,
}

/// The possible types of values in JSON Schema documents.
///
/// See [JSON Schema 4.2.1. Instance Data Model](https://tools.ietf.org/html/draft-handrews-json-schema-02#section-4.2.1).
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema)]
#[serde(rename_all = "camelCase")]
enum InstanceType {
    /// Represents the JSON null type.
    Null,
    /// Represents the JSON boolean type.
    Boolean,
    /// Represents the JSON object type.
    Object,
    /// Represents the JSON array type.
    Array,
    /// Represents the JSON number type (floating point).
    Number,
    /// Represents the JSON string type.
    String,
    /// Represents the JSON integer type.
    Integer,
}

/// A type which can be serialized as a single item, or multiple items.
///
/// In some contexts, a `Single` may be semantically distinct from a `Vec` containing only item.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, JsonSchema)]
#[serde(untagged)]
enum SingleOrVec<T> {
    /// Represents a single item.
    Single(Box<T>),
    /// Represents a vector of items.
    Vec(Vec<T>),
}

// #[cfg(test)]
// mod test {
//     use assert_json_diff::assert_json_eq;
//     use schemars::{json_schema, schema_for, JsonSchema};
//     use serde::{Deserialize, Serialize};

//     use super::*;

//     /// A very simple enum with unit variants, and no comments
//     #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
//     enum NormalEnumNoComments {
//         A,
//         B,
//     }

//     /// A very simple enum with unit variants, and comments
//     #[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
//     enum NormalEnum {
//         /// First variant
//         A,
//         /// Second variant
//         B,

//         // No doc-comments on these variants
//         C,
//         D,
//     }

//     #[test]
//     fn schema_for_enum_without_comments() {
//         let schemars_schema = schema_for!(NormalEnumNoComments);

//         assert_json_eq!(
//             schemars_schema,
//             // replace the json_schema with this to get the full output.
//             // serde_json::json!(42)
//             json_schema!(
//                 {
//                     "$schema": "https://json-schema.org/draft/2020-12/schema",
//                     "description": "A very simple enum with unit variants, and no comments",
//                     "enum": [
//                       "A",
//                       "B"
//                     ],
//                     "title": "NormalEnumNoComments",
//                     "type": "string"
//                 }
//             )
//         );

//         let kube_schema: crate::schema::Schema =
//             schemars_schema_to_kube_schema(schemars_schema.clone()).unwrap();

//         let hoisted_kube_schema = hoist_one_of_enum(kube_schema.clone());

//         // No hoisting needed
//         assert_json_eq!(hoisted_kube_schema, kube_schema);
//     }

//     #[test]
//     fn schema_for_enum_with_comments() {
//         let schemars_schema = schema_for!(NormalEnum);

//         assert_json_eq!(
//             schemars_schema,
//             // replace the json_schema with this to get the full output.
//             // serde_json::json!(42)
//             json_schema!(
//                 {
//                     "$schema": "https://json-schema.org/draft/2020-12/schema",
//                     "description": "A very simple enum with unit variants, and comments",
//                     "oneOf": [
//                       {
//                         "enum": [
//                           "C",
//                           "D"
//                         ],
//                         "type": "string"
//                       },
//                       {
//                         "const": "A",
//                         "description": "First variant",
//                         "type": "string"
//                       },
//                       {
//                         "const": "B",
//                         "description": "Second variant",
//                         "type": "string"
//                       }
//                     ],
//                     "title": "NormalEnum"
//                   }
//             )
//         );


//         let kube_schema: crate::schema::Schema =
//             schemars_schema_to_kube_schema(schemars_schema.clone()).unwrap();

//         let hoisted_kube_schema = hoist_one_of_enum(kube_schema.clone());

//         assert_ne!(
//             hoisted_kube_schema, kube_schema,
//             "Hoisting was performed, so hoisted_kube_schema != kube_schema"
//         );
//         assert_json_eq!(
//             hoisted_kube_schema,
//             json_schema!(
//                 {
//                     "$schema": "https://json-schema.org/draft/2020-12/schema",
//                     "description": "A very simple enum with unit variants, and comments",
//                     "type": "string",
//                     "enum": [
//                         "C",
//                         "D",
//                         "A",
//                         "B"
//                     ],
//                     "title": "NormalEnum"
//                   }
//             )
//         );
//     }
// }

#[cfg(test)]
fn schemars_schema_to_kube_schema(incoming: schemars::Schema) -> Result<Schema, serde_json::Error> {
    serde_json::from_value(incoming.to_value())
}

/// Hoist `oneOf` into top level `enum`.
///
/// This will move all `enum` variants and `const` values under `oneOf` into a single top level `enum` along with `type`.
/// It will panic if there are anomalies, like differences in `type` values, or lack of `enum` or `const` fields in the `oneOf` entries.
///
/// Note: variant descriptions will be lost in the process, and the original `oneOf` will be erased.
///
// Note: This function is heavily documented to express intent. It is intended to help developers
// make adjustments for future Schemars changes.
fn hoist_one_of_enum(incoming: SchemaObject) -> SchemaObject {
    // Run some initial checks in case there is nothing to do
    let SchemaObject {
        subschemas: Some(subschemas),
        ..
    } = &incoming
    else {
        return incoming;
    };

    let SubschemaValidation {
        one_of: Some(one_of), ..
    } = subschemas.deref()
    else {
        return incoming;
    };

    if one_of.is_empty() {
        return incoming;
    }

    // At this point, we need to create a new Schema and hoist the `oneOf`
    // variants' `enum`/`const` values up into a parent `enum`.
    let mut new_schema = incoming.clone();
    if let SchemaObject {
        subschemas: Some(new_subschemas),
        instance_type: new_instance_type,
        enum_values: new_enum_values,
        ..
    } = &mut new_schema
    {
        // For each `oneOf`, get the `type`.
        // Panic if it has no `type`, or if the entry is a boolean.
        let mut types = one_of.iter().map(|obj| match obj {
            Schema::Object(SchemaObject {
                instance_type: Some(r#type),
                ..
            }) => r#type,
            // TODO (@NickLarsenNZ): Is it correct that JSON Schema oneOf must have a type?
            Schema::Object(_) => panic!("oneOf variants need to define a type!: {obj:?}"),
            Schema::Bool(_) => panic!("oneOf variants can not be of type boolean"),
        });

        // Get the first `type` value, then panic if any subsequent `type` values differ.
        let hoisted_instance_type = types
            .next()
            .expect("oneOf must have at least one variant - we already checked that");
        // TODO (@NickLarsenNZ): Didn't sbernauer say that the types
        if types.any(|t| t != hoisted_instance_type) {
            panic!("All oneOf variants must have the same type");
        }

        *new_instance_type = Some(hoisted_instance_type.clone());

        // For each `oneOf` entry, iterate over the `enum` and `const` values.
        // Panic on an entry that doesn't contain an `enum` or `const`.
        let new_enums = one_of.iter().flat_map(|obj| match obj {
            Schema::Object(SchemaObject {
                enum_values: Some(r#enum),
                ..
            }) => r#enum.clone(),
            // Warning: The `const` check below must come after the enum check above.
            // Otherwise it will panic on a valid entry with an `enum`.
            Schema::Object(SchemaObject { other, .. }) => match other.get("const") {
                Some(r#const) => vec![r#const.clone()],
                None => panic!("oneOf variant did not provide \"enum\" or \"const\": {obj:?}"),
            },
            Schema::Bool(_) => panic!("oneOf variants can not be of type boolean"),
        });

        // Just in case there were existing enum values, add to them.
        // TODO (@NickLarsenNZ): Check if `oneOf` and `enum` are mutually exclusive for a valid spec.
        new_enum_values.get_or_insert_default().extend(new_enums);

        // We can clear out the existing oneOf's, since they will be hoisted below.
        new_subschemas.one_of = None;
    }

    new_schema
}

// if anyOf with 2 entries, and one is nullable with enum that is [null],
// then hoist nullable, description, type, enum from the other entry.
// set anyOf to None
fn hoist_any_of_option_enum(incoming: SchemaObject) -> SchemaObject {
    // Run some initial checks in case there is nothing to do
    let SchemaObject {
        subschemas: Some(subschemas),
        ..
    } = &incoming
    else {
        return incoming;
    };

    let SubschemaValidation {
        any_of: Some(any_of), ..
    } = subschemas.deref()
    else {
        return incoming;
    };

    if any_of.len() != 2 {
        return incoming;
    };

    // This is the signature of an Optional enum that needs hoisting
    let null = json!({
        "enum": [null],
        "nullable": true
    });

    // iter through any_of for matching null
    let results: [bool; 2] = any_of
        .iter()
        .map(|x| serde_json::to_value(x).expect("schema should be able to convert to JSON"))
        .map(|x| x == null)
        .collect::<Vec<_>>()
        .try_into()
        .expect("there should be exactly 2 elements. We checked earlier");

    let to_hoist = match results {
        [true, true] => panic!("Too many nulls, not enough drinks"),
        [true, false] => &any_of[1],
        [false, true] => &any_of[0],
        [false, false] => return incoming,
    };

    // my goodness!
    let Schema::Object(to_hoist) = to_hoist else {
        panic!("Somehow we have stumbled across a bool schema");
    };

    let mut new_schema = incoming.clone();

    let mut new_metadata = incoming.metadata.clone().unwrap_or_default();
    new_metadata.description = to_hoist.metadata.as_ref().and_then(|m| m.description.clone());

    new_schema.metadata = Some(new_metadata);
    new_schema.instance_type = to_hoist.instance_type.clone();
    new_schema.enum_values = to_hoist.enum_values.clone();
    new_schema.other["nullable"] = true.into();

    new_schema
        .subschemas
        .as_mut()
        .expect("we have asserted that there is any_of")
        .any_of = None;

    new_schema
}


impl Transform for StructuralSchemaRewriter {
    fn transform(&mut self, transform_schema: &mut schemars::Schema) {
        schemars::transform::transform_subschemas(self, transform_schema);

        // TODO (@NickLarsenNZ): Replace with conversion function
        let schema: SchemaObject = match serde_json::from_value(transform_schema.clone().to_value()).ok() {
            Some(schema) => schema,
            None => return,
        };
        let schema = hoist_one_of_enum(schema);
        let schema = hoist_any_of_option_enum(schema);
        // todo: let schema = strip_any_of_empty_object_entry(schema);
        let mut schema = schema;
        if let Some(subschemas) = &mut schema.subschemas {
            if let Some(one_of) = subschemas.one_of.as_mut() {
                // Tagged enums are serialized using `one_of`
                hoist_subschema_properties(one_of, &mut schema.object, &mut schema.instance_type);

                // "Plain" enums are serialized using `one_of` if they have doc tags
                hoist_subschema_enum_values(one_of, &mut schema.enum_values, &mut schema.instance_type);

                if one_of.is_empty() {
                    subschemas.one_of = None;
                }
            }

            if let Some(any_of) = &mut subschemas.any_of {
                // Untagged enums are serialized using `any_of`
                hoist_subschema_properties(any_of, &mut schema.object, &mut schema.instance_type);
            }
        }

        // check for maps without with properties (i.e. flattened maps)
        // and allow these to persist dynamically
        if let Some(object) = &mut schema.object {
            if !object.properties.is_empty()
                && object.additional_properties.as_deref() == Some(&Schema::Bool(true))
            {
                object.additional_properties = None;
                schema
                    .extensions
                    .insert("x-kubernetes-preserve-unknown-fields".into(), true.into());
            }
        }

        // As of version 1.30 Kubernetes does not support setting `uniqueItems` to `true`,
        // so we need to remove this fields.
        // Users can still set `x-kubernetes-list-type=set` in case they want the apiserver
        // to do validation, but we can't make an assumption about the Set contents here.
        // See https://kubernetes.io/docs/reference/using-api/server-side-apply/ for details.
        if let Some(array) = &mut schema.array {
            array.unique_items = None;
        }

        if let Ok(schema) = serde_json::to_value(schema) {
            if let Ok(transformed) = serde_json::from_value(schema) {
                *transform_schema = transformed;
            }
        }
    }
}

/// Bring all plain enum values up to the root schema,
/// since Kubernetes doesn't allow subschemas to define enum options.
///
/// (Enum here means a list of hard-coded values, not a tagged union.)
fn hoist_subschema_enum_values(
    subschemas: &mut Vec<Schema>,
    common_enum_values: &mut Option<Vec<serde_json::Value>>,
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
) {
    subschemas.retain(|variant| {
        if let Schema::Object(SchemaObject {
            instance_type: variant_type,
            enum_values: Some(variant_enum_values),
            ..
        }) = variant
        {
            if let Some(variant_type) = variant_type {
                match instance_type {
                    None => *instance_type = Some(variant_type.clone()),
                    Some(tpe) => {
                        if tpe != variant_type {
                            panic!("Enum variant set {variant_enum_values:?} has type {variant_type:?} but was already defined as {instance_type:?}. The instance type must be equal for all subschema variants.")
                        }
                    }
                }
            }
            common_enum_values
                .get_or_insert_with(Vec::new)
                .extend(variant_enum_values.iter().cloned());
            false
        } else {
            true
        }
    })
}

/// Bring all property definitions from subschemas up to the root schema,
/// since Kubernetes doesn't allow subschemas to define properties.
fn hoist_subschema_properties(
    subschemas: &mut Vec<Schema>,
    common_obj: &mut Option<Box<ObjectValidation>>,
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
) {
    for variant in subschemas {
        if let Schema::Object(SchemaObject {
            instance_type: variant_type,
            object: Some(variant_obj),
            metadata: variant_metadata,
            ..
        }) = variant
        {
            let common_obj = common_obj.get_or_insert_with(Box::<ObjectValidation>::default);

            if let Some(variant_metadata) = variant_metadata {
                // Move enum variant description from oneOf clause to its corresponding property
                if let Some(description) = std::mem::take(&mut variant_metadata.description) {
                    if let Some(Schema::Object(variant_object)) =
                        only_item(variant_obj.properties.values_mut())
                    {
                        let metadata = variant_object
                            .metadata
                            .get_or_insert_with(Box::<Metadata>::default);
                        metadata.description = Some(description);
                    }
                }
            }

            // Move all properties
            let variant_properties = std::mem::take(&mut variant_obj.properties);
            for (property_name, property) in variant_properties {
                match common_obj.properties.entry(property_name) {
                    Entry::Vacant(entry) => {
                        entry.insert(property);
                    }
                    Entry::Occupied(entry) => {
                        if &property != entry.get() {
                            panic!("Property {:?} has the schema {:?} but was already defined as {:?} in another subschema. The schemas for a property used in multiple subschemas must be identical",
                            entry.key(),
                            &property,
                            entry.get());
                        }
                    }
                }
            }

            // Kubernetes doesn't allow variants to set additionalProperties
            variant_obj.additional_properties = None;

            merge_metadata(instance_type, variant_type.take());
        } else if let Schema::Object(SchemaObject {
            object: None,
            instance_type: variant_type,
            metadata,
            ..
        }) = variant
        {
            if *variant_type == Some(SingleOrVec::Single(Box::new(InstanceType::Object))) {
                *variant_type = None;
                *metadata = None;
            }
        }
    }
}

fn only_item<I: Iterator>(mut i: I) -> Option<I::Item> {
    let item = i.next()?;
    if i.next().is_some() {
        return None;
    }
    Some(item)
}

fn merge_metadata(
    instance_type: &mut Option<SingleOrVec<InstanceType>>,
    variant_type: Option<SingleOrVec<InstanceType>>,
) {
    match (instance_type, variant_type) {
        (_, None) => {}
        (common_type @ None, variant_type) => {
            *common_type = variant_type;
        }
        (Some(common_type), Some(variant_type)) => {
            if *common_type != variant_type {
                panic!(
                    "variant defined type {variant_type:?}, conflicting with existing type {common_type:?}"
                );
            }
        }
    }
}
