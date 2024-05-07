use std::iter;
use std::fmt::Debug;

use arrow::array::{Array, FixedSizeListArray, ListArray, MapArray, StructArray};
use arrow::bitmap::Bitmap;
use arrow::datatypes::PhysicalType;
use arrow::offset::{Offset, OffsetsBuffer};
use polars_error::{polars_bail, PolarsResult};

use super::{array_to_pages, Encoding, WriteOptions};
use crate::arrow::read::schema::is_nullable;
use crate::parquet::page::Page;
use crate::parquet::schema::types::{ParquetType, PrimitiveType as ParquetPrimitiveType};
use crate::write::DynIter;
use crate::arrow::write::nested::num_values;

/// Constructs iterators for rep and def levels of `array`
pub fn to_levels(nested: &[Nested]
) -> PolarsResult<(Vec<u32>, Vec<u32>)> {
    if nested.len() == 0 {
        return Ok((vec![], vec![]));
    }
    let value_count = num_values(nested);
    let mut def_level = Vec::with_capacity(value_count);
    let mut rep_level = Vec::with_capacity(value_count);

    to_levels_recursive(nested, &mut def_level, &mut rep_level, 0, 0, 0, 0, nested[0].len())?;
    Ok((def_level, rep_level))
}

fn to_levels_recursive(
    nested: &[Nested],
    def_level: &mut Vec<u32>,
    rep_level: &mut Vec<u32>,
    current_level: u32,
    parent_level: u32,
    validity_bonus: u32,
    offset: usize,
    length: usize,
) -> PolarsResult<()> {
    let current_nested = &nested[0];
    match current_nested {
        Nested::Primitive(validity, is_optional, _) => {
            if length == 0 {
                def_level.push(parent_level + validity_bonus);
                rep_level.push(parent_level);
                return Ok(());
            }
            match validity {
                Some(bitmap) => {
                    let mut bitmap_sliced = bitmap.clone();
                    bitmap_sliced.slice(offset, length);
                    def_level.extend(
                        bitmap_sliced.iter()
                            .zip(iter::repeat(current_level + validity_bonus))
                            .map(|(is_valid, def_null)| def_null + is_valid as u32)
                            .take(bitmap.len()));
                }
                None => {
                    def_level.extend(iter::repeat(current_level + *is_optional as u32 + validity_bonus).take(length));
                }
            }
            rep_level.push(parent_level);
            if length > 1 {
                rep_level.extend(
                    iter::repeat(current_level).take(length - 1)
                );
            }
        }
        Nested::List(list_nested) => {
            if length == 0 {
                def_level.push(parent_level + validity_bonus);
                rep_level.push(parent_level);
                return Ok(());
            }
            let mut sliced_offsets = list_nested.offsets.clone();
            sliced_offsets.slice(offset, length + 1);
            let next_level = current_level + list_nested.is_optional as u32;
            // List fields get auto +1 def level (see FixedSizeList match arm)
            let next_validity_bonus = validity_bonus + 1;
            // Fields are nullable if array has bitmap
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = bitmap.iter();
                // First element has repetition level = parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        let (start, end) = sliced_offsets.start_end(0);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, start, end - start)?;
                    }
                    Some(false) => {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(parent_level);
                    }
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    }
                }
                // Subsequent elements have repetition level = current level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, start, end - start)?;
                    } else {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(current_level);
                    }
                }
            } else {
                let (start, end) = sliced_offsets.start_end(0);
                to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, start, end - start)?;
                if length > 1 {
                    for i in 1..length {
                        let (start, end) = sliced_offsets.start_end(i);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, start, end - start)?;
                    }
                }
            }
        }
        Nested::LargeList(list_nested) => {
            if length == 0 {
                def_level.push(parent_level + validity_bonus);
                rep_level.push(parent_level);
                return Ok(());
            }
            let mut sliced_offsets = list_nested.offsets.clone();
            sliced_offsets.slice(offset, length + 1);
            let next_level = current_level + list_nested.is_optional as u32;
            // List fields get auto +1 def level (see FixedSizeList match arm)
            let next_validity_bonus = validity_bonus + 1;
            // Fields are nullable if array has bitmap
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = bitmap.iter();
                // First element has repetition level = parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        let (start, end) = sliced_offsets.start_end(0);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, start, end - start)?;
                    }
                    Some(false) => {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(parent_level);
                    }
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    }
                }
                // Subsequent elements have repetition level = current level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, start, end - start)?;
                    } else {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(current_level);
                    }
                }
            } else {
                let (start, end) = sliced_offsets.start_end(0);
                to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, start, end - start)?;
                if length > 1 {
                    for i in 1..length {
                        let (start, end) = sliced_offsets.start_end(i);
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, start, end - start)?;
                    }
                }
            }
        }
        Nested::Struct(validity, is_optional, ..) => {
            if length == 0 {
                def_level.push(parent_level + validity_bonus);
                rep_level.push(parent_level);
                return Ok(());
            }
            let next_level = current_level + *is_optional as u32;
            // Fields are nullable if array has bitmap
            if let Some(bitmap) = validity {
                let mut bitmap_iter = bitmap.iter();
                // First element has repetition level = parent level
                if let Some(true) = bitmap_iter.next() {
                    to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, validity_bonus, offset, 1)?;
                } else {
                    def_level.push(parent_level + validity_bonus);
                    rep_level.push(parent_level);
                }
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, validity_bonus, offset + i + 1, 1)?;
                    } else {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(current_level);
                    }
                }
            } else {
                to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, validity_bonus, offset, 1)?;
                if length > 1 {
                    for i in 1..length {
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, validity_bonus, offset+ i, 1)?;
                    }
                }
            }
        }
        Nested::FixedSizeList {is_optional, width, validity, ..} => {
            if length == 0 {
                def_level.push(parent_level + validity_bonus);
                rep_level.push(parent_level);
                return Ok(());
            }
            let next_level = current_level + *is_optional as u32;
            // List fields consist of two nested fields: outer is group of 
            // lists and inner is group of elements. Non-null elements get an
            // +1 definition level in addition to normal bump from is_optional.
            let next_validity_bonus = validity_bonus + 1;
            // Fields are nullable if array has bitmap
            if let Some(bitmap) = validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = sliced_bitmap.iter();
                // First element has repetition level = parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, 0, *width)?;
                    }
                    Some(false) => {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(parent_level);
                    }
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    }
                }
                // Subsequent elements have repetition level = current level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, width * (i+1), *width)?;
                    } else {
                        def_level.push(parent_level + validity_bonus);
                        rep_level.push(current_level);
                    }
                }
            } else {
                to_levels_recursive(&nested[1..], def_level, rep_level, next_level, parent_level, next_validity_bonus, 0, *width)?;
                if length > 1 {
                    for i in 1..length {
                        to_levels_recursive(&nested[1..], def_level, rep_level, next_level, current_level, next_validity_bonus, i * width, *width)?;
                    }
                }
            }
        }
    };
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListNested<O: Offset> {
    pub is_optional: bool,
    pub offsets: OffsetsBuffer<O>,
    pub validity: Option<Bitmap>,
}

impl<O: Offset> ListNested<O> {
    pub fn new(offsets: OffsetsBuffer<O>, validity: Option<Bitmap>, is_optional: bool) -> Self {
        Self {
            is_optional,
            offsets,
            validity,
        }
    }
}

/// Descriptor of nested information of a field
#[derive(Debug, Clone, PartialEq)]
pub enum Nested {
    /// a primitive (leaf or parquet column)
    /// - validity
    /// - is_optional
    /// - length
    Primitive(Option<Bitmap>, bool, usize),
    /// a list
    List(ListNested<i32>),
    /// a list
    LargeList(ListNested<i64>),
    /// Width
    FixedSizeList {
        validity: Option<Bitmap>,
        is_optional: bool,
        width: usize,
        len: usize,
    },
    /// a struct
    /// - validity
    /// - is_optional
    /// - length
    Struct(Option<Bitmap>, bool, usize),
}

impl Nested {
    /// Returns the length (number of rows) of the element
    pub fn len(&self) -> usize {
        match self {
            Nested::Primitive(_, _, length) => *length,
            Nested::List(nested) => nested.offsets.len_proxy(),
            Nested::LargeList(nested) => nested.offsets.len_proxy(),
            Nested::Struct(_, _, len) => *len,
            Nested::FixedSizeList { len, .. } => *len,
        }
    }
}

/// Constructs the necessary `Vec<Vec<Nested>>` to write the rep and def levels of `array` to parquet
pub fn to_nested(array: &dyn Array, type_: &ParquetType) -> PolarsResult<Vec<Vec<Nested>>> {
    let mut nested = vec![];

    to_nested_recursive(array, type_, &mut nested, vec![])?;
    Ok(nested)
}

fn to_nested_recursive(
    array: &dyn Array,
    type_: &ParquetType,
    nested: &mut Vec<Vec<Nested>>,
    mut parents: Vec<Nested>,
) -> PolarsResult<()> {
    let is_optional = is_nullable(type_.get_field_info());

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let fields = if let ParquetType::GroupType { fields, .. } = type_ {
                fields
            } else {
                polars_bail!(InvalidOperation:
                    "Parquet type must be a group for a struct array".to_string(),
                )
            };

            parents.push(Nested::Struct(
                array.validity().cloned(),
                is_optional,
                array.len(),
            ));

            for (type_, array) in fields.iter().zip(array.values()) {
                to_nested_recursive(array.as_ref(), type_, nested, parents.clone())?;
            }
        },
        FixedSizeList => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    polars_bail!(InvalidOperation:
                        "Parquet type must be a group for a list array".to_string(),
                    )
                }
            } else {
                polars_bail!(InvalidOperation:
                    "Parquet type must be a group for a list array".to_string(),
                )
            };

            parents.push(Nested::FixedSizeList {
                validity: array.validity().cloned(),
                len: array.len(),
                width: array.size(),
                is_optional,
            });
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        },
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    polars_bail!(InvalidOperation:
                        "Parquet type must be a group for a list array".to_string(),
                    )
                }
            } else {
                polars_bail!(InvalidOperation:
                    "Parquet type must be a group for a list array".to_string(),
                )
            };

            parents.push(Nested::List(ListNested::new(
                array.offsets().clone(),
                array.validity().cloned(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        },
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    polars_bail!(InvalidOperation:
                        "Parquet type must be a group for a list array".to_string(),
                    )
                }
            } else {
                polars_bail!(InvalidOperation:
                    "Parquet type must be a group for a list array".to_string(),
                )
            };

            parents.push(Nested::LargeList(ListNested::new(
                array.offsets().clone(),
                array.validity().cloned(),
                is_optional,
            )));
            to_nested_recursive(array.values().as_ref(), type_, nested, parents)?;
        },
        Map => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            let type_ = if let ParquetType::GroupType { fields, .. } = type_ {
                if let ParquetType::GroupType { fields, .. } = &fields[0] {
                    &fields[0]
                } else {
                    polars_bail!(InvalidOperation:
                        "Parquet type must be a group for a map array".to_string(),
                    )
                }
            } else {
                polars_bail!(InvalidOperation:
                    "Parquet type must be a group for a map array".to_string(),
                )
            };

            parents.push(Nested::List(ListNested::new(
                array.offsets().clone(),
                array.validity().cloned(),
                is_optional,
            )));
            to_nested_recursive(array.field().as_ref(), type_, nested, parents)?;
        },
        _ => {
            parents.push(Nested::Primitive(
                array.validity().cloned(),
                is_optional,
                array.len(),
            ));
            nested.push(parents)
        },
    }
    Ok(())
}

/// Convert [`Array`] to `Vec<&dyn Array>` leaves in DFS order.
pub fn to_leaves(array: &dyn Array) -> Vec<&dyn Array> {
    let mut leaves = vec![];
    to_leaves_recursive(array, &mut leaves);
    leaves
}

fn to_leaves_recursive<'a>(array: &'a dyn Array, leaves: &mut Vec<&'a dyn Array>) {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Struct => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            array
                .values()
                .iter()
                .for_each(|a| to_leaves_recursive(a.as_ref(), leaves));
        },
        List => {
            let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        },
        LargeList => {
            let array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        },
        FixedSizeList => {
            let array = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
            to_leaves_recursive(array.values().as_ref(), leaves);
        },
        Map => {
            let array = array.as_any().downcast_ref::<MapArray>().unwrap();
            to_leaves_recursive(array.field().as_ref(), leaves);
        },
        Null | Boolean | Primitive(_) | Binary | FixedSizeBinary | LargeBinary | Utf8
        | LargeUtf8 | Dictionary(_) | BinaryView | Utf8View => leaves.push(array),
        other => todo!("Writing {:?} to parquet not yet implemented", other),
    }
}

/// Convert `ParquetType` to `Vec<ParquetPrimitiveType>` leaves in DFS order.
pub fn to_parquet_leaves(type_: ParquetType) -> Vec<ParquetPrimitiveType> {
    let mut leaves = vec![];
    to_parquet_leaves_recursive(type_, &mut leaves);
    leaves
}

fn to_parquet_leaves_recursive(type_: ParquetType, leaves: &mut Vec<ParquetPrimitiveType>) {
    match type_ {
        ParquetType::PrimitiveType(primitive) => leaves.push(primitive),
        ParquetType::GroupType { fields, .. } => {
            fields
                .into_iter()
                .for_each(|type_| to_parquet_leaves_recursive(type_, leaves));
        },
    }
}

/// Returns a vector of iterators of [`Page`], one per leaf column in the array
pub fn array_to_columns<A: AsRef<dyn Array> + Send + Sync>(
    array: A,
    type_: ParquetType,
    options: WriteOptions,
    encoding: &[Encoding],
) -> PolarsResult<Vec<DynIter<'static, PolarsResult<Page>>>> {
    let array = array.as_ref();
    let nested = to_nested(array, &type_)?;

    let types = to_parquet_leaves(type_);

    let values = to_leaves(array);

    assert_eq!(encoding.len(), types.len());
    
    values
        .iter()
        .zip(nested)
        .zip(types)
        .zip(encoding.iter())
        .map(|(((values, nested), type_), encoding)| {
            if let Ok((def_level, rep_level)) = to_levels(&nested) {
                array_to_pages(*values, type_, &nested, options, *encoding, def_level, rep_level)
            } else {
                polars_bail!(InvalidOperation:
                    "Something went wrong getting rep / def levels".to_string(),
                )
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use arrow::datatypes::*;

    use super::super::{FieldInfo, ParquetPhysicalType};
    use super::*;
    use crate::parquet::schema::types::{
        GroupLogicalType, PrimitiveConvertedType, PrimitiveLogicalType,
    };
    use crate::parquet::schema::Repetition;

    #[test]
    fn test_struct() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", ArrowDataType::Boolean, false),
            Field::new("c", ArrowDataType::Int32, false),
        ];

        let array = StructArray::new(
            ArrowDataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };
        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                vec![
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }

    #[test]
    fn test_struct_struct() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", ArrowDataType::Boolean, false),
            Field::new("c", ArrowDataType::Int32, false),
        ];

        let array = StructArray::new(
            ArrowDataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let fields = vec![
            Field::new("b", array.data_type().clone(), true),
            Field::new("c", array.data_type().clone(), true),
        ];

        let array = StructArray::new(
            ArrowDataType::Struct(fields),
            vec![Box::new(array.clone()), Box::new(array)],
            None,
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![type_.clone(), type_],
        };

        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                // a.b.b
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.b.c
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.c.b
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                // a.c.c
                vec![
                    Nested::Struct(None, false, 4),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }

    #[test]
    fn test_list_struct() {
        let boolean = BooleanArray::from_slice([false, false, true, true]).boxed();
        let int = Int32Array::from_slice([42, 28, 19, 31]).boxed();

        let fields = vec![
            Field::new("b", ArrowDataType::Boolean, false),
            Field::new("c", ArrowDataType::Int32, false),
        ];

        let array = StructArray::new(
            ArrowDataType::Struct(fields),
            vec![boolean.clone(), int.clone()],
            Some(Bitmap::from([true, true, false, true])),
        );

        let array = ListArray::new(
            ArrowDataType::List(Box::new(Field::new("l", array.data_type().clone(), true))),
            vec![0i32, 2, 4].try_into().unwrap(),
            Box::new(array),
            None,
        );

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "a".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "b".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Boolean,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "c".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "l".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![ParquetType::GroupType {
                field_info: FieldInfo {
                    name: "list".to_string(),
                    repetition: Repetition::Repeated,
                    id: None,
                },
                logical_type: None,
                converted_type: None,
                fields: vec![type_],
            }],
        };

        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: vec![0, 2, 4].try_into().unwrap(),
                        validity: None,
                    }),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: vec![0, 2, 4].try_into().unwrap(),
                        validity: None,
                    }),
                    Nested::Struct(Some(Bitmap::from([true, true, false, true])), true, 4),
                    Nested::Primitive(None, false, 4),
                ],
            ]
        );
    }

    #[test]
    fn test_map() {
        let kv_type = ArrowDataType::Struct(vec![
            Field::new("k", ArrowDataType::Utf8, false),
            Field::new("v", ArrowDataType::Int32, false),
        ]);
        let kv_field = Field::new("kv", kv_type.clone(), false);
        let map_type = ArrowDataType::Map(Box::new(kv_field), false);

        let key_array = Utf8Array::<i32>::from_slice(["k1", "k2", "k3", "k4", "k5", "k6"]).boxed();
        let val_array = Int32Array::from_slice([42, 28, 19, 31, 21, 17]).boxed();
        let kv_array = StructArray::try_new(kv_type, vec![key_array, val_array], None)
            .unwrap()
            .boxed();
        let offsets = OffsetsBuffer::try_from(vec![0, 2, 3, 4, 6]).unwrap();

        let array = MapArray::try_new(map_type, offsets, kv_array, None).unwrap();

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "kv".to_string(),
                repetition: Repetition::Optional,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            fields: vec![
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "k".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: Some(PrimitiveLogicalType::String),
                    converted_type: Some(PrimitiveConvertedType::Utf8),
                    physical_type: ParquetPhysicalType::ByteArray,
                }),
                ParquetType::PrimitiveType(ParquetPrimitiveType {
                    field_info: FieldInfo {
                        name: "v".to_string(),
                        repetition: Repetition::Required,
                        id: None,
                    },
                    logical_type: None,
                    converted_type: None,
                    physical_type: ParquetPhysicalType::Int32,
                }),
            ],
        };

        let type_ = ParquetType::GroupType {
            field_info: FieldInfo {
                name: "m".to_string(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: Some(GroupLogicalType::Map),
            converted_type: None,
            fields: vec![ParquetType::GroupType {
                field_info: FieldInfo {
                    name: "map".to_string(),
                    repetition: Repetition::Repeated,
                    id: None,
                },
                logical_type: None,
                converted_type: None,
                fields: vec![type_],
            }],
        };

        let a = to_nested(&array, &type_).unwrap();

        assert_eq!(
            a,
            vec![
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: vec![0, 2, 3, 4, 6].try_into().unwrap(),
                        validity: None,
                    }),
                    Nested::Struct(None, true, 6),
                    Nested::Primitive(None, false, 6),
                ],
                vec![
                    Nested::List(ListNested::<i32> {
                        is_optional: false,
                        offsets: vec![0, 2, 3, 4, 6].try_into().unwrap(),
                        validity: None,
                    }),
                    Nested::Struct(None, true, 6),
                    Nested::Primitive(None, false, 6),
                ],
            ]
        );
    }
}
