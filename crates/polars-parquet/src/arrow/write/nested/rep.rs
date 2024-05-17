use polars_error::{polars_bail, PolarsResult};

use super::super::pages::Nested;
use super::to_length;

/// Constructs iterators for rep levels of `array`
pub fn calculate_rep_levels(nested: &[Nested], value_count: usize) -> PolarsResult<Vec<u32>> {
    if nested.is_empty() {
        return Ok(vec![]);
    }
    let mut rep_levels = Vec::with_capacity(value_count);

    rep_levels_recursive(nested, &mut rep_levels, 0, 0, 0, nested[0].len())?;
    Ok(rep_levels)
}

fn rep_levels_recursive(
    nested: &[Nested],
    rep_levels: &mut Vec<u32>,
    current_level: u32,
    parent_level: u32,
    offset: usize,
    length: usize,
) -> PolarsResult<()> {
    if length == 0 {
        rep_levels.push(parent_level);
        return Ok(());
    }
    let current_nested = &nested[0];
    match current_nested {
        Nested::Primitive(..) => {
            rep_levels.push(parent_level);
            rep_levels.extend(std::iter::repeat(current_level).take(length - 1));
        },
        Nested::List(list_nested) => {
            let mut sliced_offsets = list_nested.offsets.clone();
            // Inner values are already sliced so subtract first offset
            let first_offset = *sliced_offsets.first() as usize;
            sliced_offsets.slice(offset, length + 1);
            let next_level = current_level + list_nested.is_optional as u32;
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = sliced_bitmap.iter();
                // First element inherits parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        let (start, end) = sliced_offsets.start_end(0);
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            parent_level,
                            start - first_offset,
                            end - start,
                        )?;
                    },
                    Some(false) => {
                        rep_levels.push(parent_level);
                    },
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    },
                }
                // Subsequent elements take current level as parent level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i + 1);
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            current_level,
                            start - first_offset,
                            end - start,
                        )?;
                    } else {
                        rep_levels.push(current_level);
                    }
                }
            } else {
                let (start, end) = sliced_offsets.start_end(0);
                rep_levels_recursive(
                    &nested[1..],
                    rep_levels,
                    next_level,
                    parent_level,
                    start - first_offset,
                    end - start,
                )?;
                for i in 1..length {
                    let (start, end) = sliced_offsets.start_end(i);
                    rep_levels_recursive(
                        &nested[1..],
                        rep_levels,
                        next_level,
                        current_level,
                        start - first_offset,
                        end - start,
                    )?;
                }
            }
        },
        Nested::LargeList(list_nested) => {
            let mut sliced_offsets = list_nested.offsets.clone();
            // Inner values are already sliced so subtract first offset
            let first_offset = *sliced_offsets.first() as usize;
            sliced_offsets.slice(offset, length + 1);
            let next_level = current_level + list_nested.is_optional as u32;
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = sliced_bitmap.iter();
                // First element inherits parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        let (start, end) = sliced_offsets.start_end(0);
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            parent_level,
                            start - first_offset,
                            end - start,
                        )?;
                    },
                    Some(false) => {
                        rep_levels.push(parent_level);
                    },
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    },
                }
                // Subsequent elements take current level as parent level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i + 1);
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            current_level,
                            start - first_offset,
                            end - start,
                        )?;
                    } else {
                        rep_levels.push(current_level);
                    }
                }
            } else {
                let (start, end) = sliced_offsets.start_end(0);
                rep_levels_recursive(
                    &nested[1..],
                    rep_levels,
                    next_level,
                    parent_level,
                    start - first_offset,
                    end - start,
                )?;
                for i in 1..length {
                    let (start, end) = sliced_offsets.start_end(i);
                    rep_levels_recursive(
                        &nested[1..],
                        rep_levels,
                        next_level,
                        current_level,
                        start - first_offset,
                        end - start,
                    )?;
                }
            }
        },
        Nested::Struct(validity, ..) => {
            if let Some(bitmap) = validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = sliced_bitmap.iter();
                // First element inherits parent level
                if let Some(true) = bitmap_iter.next() {
                    rep_levels_recursive(
                        &nested[1..],
                        rep_levels,
                        current_level,
                        parent_level,
                        offset,
                        1,
                    )?;
                } else {
                    rep_levels.push(parent_level);
                }
                // Subsequent elements take current level as parent level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            current_level,
                            current_level,
                            offset + i + 1,
                            1,
                        )?;
                    } else {
                        rep_levels.push(current_level);
                    }
                }
            } else {
                rep_levels_recursive(
                    &nested[1..],
                    rep_levels,
                    current_level,
                    parent_level,
                    offset,
                    1,
                )?;
                if length > 1 {
                    rep_levels_recursive(
                        &nested[1..],
                        rep_levels,
                        current_level,
                        current_level,
                        offset + 1,
                        length - 1,
                    )?;
                }
            }
        },
        Nested::FixedSizeList {
            is_optional,
            width,
            validity,
            ..
        } => {
            let next_level = current_level + *is_optional as u32;
            // Fields are nullable if array has bitmap
            if let Some(bitmap) = validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                let mut bitmap_iter = sliced_bitmap.iter();
                // First element has repetition level = parent level
                match bitmap_iter.next() {
                    Some(true) => {
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            parent_level,
                            0,
                            *width,
                        )?;
                    },
                    Some(false) => {
                        rep_levels.push(parent_level);
                    },
                    None => {
                        polars_bail!(InvalidOperation:
                            "Validity bitmap should not be empty".to_string(),
                        )
                    },
                }
                // Subsequent elements have repetition level = current level
                for (i, is_valid) in bitmap_iter.enumerate() {
                    if is_valid {
                        rep_levels_recursive(
                            &nested[1..],
                            rep_levels,
                            next_level,
                            current_level,
                            width * (i + 1),
                            *width,
                        )?;
                    } else {
                        rep_levels.push(current_level);
                    }
                }
            } else {
                rep_levels_recursive(
                    &nested[1..],
                    rep_levels,
                    next_level,
                    parent_level,
                    0,
                    *width,
                )?;
                for i in 1..length {
                    rep_levels_recursive(
                        &nested[1..],
                        rep_levels,
                        next_level,
                        current_level,
                        width * i,
                        *width,
                    )?;
                }
            }
        },
    };
    Ok(())
}

pub trait DebugIter: Iterator<Item = usize> + std::fmt::Debug {}

impl<A: Iterator<Item = usize> + std::fmt::Debug> DebugIter for A {}

fn iter<'a>(nested: &'a [Nested]) -> Vec<Box<dyn DebugIter + 'a>> {
    nested
        .iter()
        .filter_map(|nested| match nested {
            Nested::Primitive(_, _, _) => None,
            Nested::List(nested) => {
                Some(Box::new(to_length(&nested.offsets)) as Box<dyn DebugIter>)
            },
            Nested::LargeList(nested) => {
                Some(Box::new(to_length(&nested.offsets)) as Box<dyn DebugIter>)
            },
            Nested::FixedSizeList { width, len, .. } => {
                Some(Box::new(std::iter::repeat(*width).take(*len)) as Box<dyn DebugIter>)
            },
            Nested::Struct(_, _, _) => None,
        })
        .collect()
}

/// return number values of the nested
pub fn num_values(nested: &[Nested]) -> usize {
    let pr = match nested.last().unwrap() {
        Nested::Primitive(_, _, len) => *len,
        _ => unreachable!(),
    };

    iter(nested)
        .into_iter()
        .map(|lengths| {
            lengths
                .map(|length| if length == 0 { 1 } else { 0 })
                .sum::<usize>()
        })
        .sum::<usize>()
        + pr
}

#[cfg(test)]
mod tests {
    use super::super::super::pages::ListNested;
    use super::*;

    use test::Bencher;
    use arrow::bitmap::Bitmap;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let value_count = num_values(&nested);
        if let Ok(result) = calculate_rep_levels(&nested, value_count) {
            assert_eq!(result.len(), expected.len());
            assert_eq!(result, expected);
        } else {
            panic!("Failed to calculate rep levels.")
        }
    }

    #[test]
    fn struct_required() {
        let nested = vec![
            Nested::Struct(None, false, 10),
            Nested::Primitive(None, true, 10),
        ];
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn struct_optional() {
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(None, true, 10),
        ];
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn l1() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![0u32, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn l2() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 4].try_into().unwrap(),
                validity: None,
            }),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 3, 7, 8, 10].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 10),
        ];
        let expected = vec![0, 2, 2, 1, 2, 2, 2, 0, 0, 1, 2];

        test(nested, expected)
    }

    #[test]
    fn list_of_struct() {
        /*
        [
            [{"a": "b"}],[{"a": "c"}]
        ]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 2].try_into().unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 2),
            Nested::Primitive(None, true, 2),
        ];
        let expected = vec![0, 0];

        test(nested, expected)
    }

    #[test]
    fn list_struct_list() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 3].try_into().unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 3),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 3, 6, 7].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 7),
        ];
        let expected = vec![0, 2, 2, 1, 2, 2, 0];

        test(nested, expected)
    }

    #[test]
    fn struct_list_optional() {
        /*
        {"f1": ["a", "b", None, "c"]}
        */
        let nested = vec![
            Nested::Struct(None, true, 1),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 4].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 4),
        ];
        let expected = vec![0, 1, 1, 1];

        test(nested, expected)
    }

    #[test]
    fn l2_other() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 1, 3, 5, 5, 8, 8, 9].try_into().unwrap(),
                validity: None,
            }),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 4, 5, 7, 8, 9, 10, 11, 12].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![0, 2, 0, 0, 2, 1, 0, 2, 1, 0, 0, 1, 1, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn list_struct_list_1() {
        /*
        [
            [{"a": ["a"]}, {"a": ["b"]}],
            [],
            [{"a": ["b"]}, None, {"a": ["b"]}],
            [{"a": []}, {"a": []}, {"a": []}],
            [],
            [{"a": ["d"]}, {"a": ["a"]}, {"a": ["c", "d"]}],
            [],
            [{"a": []}],
        ]
        // reps: [0, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 2, 0, 0]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 2, 3, 3, 4, 4, 4, 4, 5, 6, 8, 8]
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 8),
        ];
        let expected = vec![0, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 2, 0, 0];

        test(nested, expected)
    }

    #[test]
    fn list_struct_list_2() {
        /*
        [
            [{"a": []}],
        ]
        // reps: [0]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1].try_into().unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 0].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 0),
        ];
        let expected = vec![0];

        test(nested, expected)
    }

    #[test]
    fn list_struct_list_3() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 1].try_into().unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 0].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 0),
        ];
        let expected = vec![0, 0];
        // [1, 0], [0]
        // pick last

        test(nested, expected)
    }

    #[bench]
    fn bench_mega_nested(b: &mut Bencher) {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![1, 2, 0, 3].iter()
                    .cycle()
                    .take(4 * 1000)
                    .scan(0, |state, &x| { *state += x; Some(*state) })
                    .collect::<Vec<i32>>()
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Struct(None, true, 6000),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![1, 2, 0, 2].iter()
                    .cycle()
                    .take(6000)
                    .scan(0, |state, &x| { *state += x; Some(*state) })
                    .collect::<Vec<i32>>()
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Struct(
                Some(Bitmap::try_new([1u8, 1, 0, 0, 1].into_iter()
                    .cycle()
                    .take(7500)
                    .collect::<Vec<u8>>(), 7500)
                    .unwrap()),
                true, 7500),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![1, 1, 0, 0, 1].iter()
                    .cycle()
                    .take(7500)
                    .scan(0, |state, &x| { *state += x; Some(*state) })
                    .collect::<Vec<i32>>()
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 4500),
        ];
        let value_count = num_values(&nested);
        b.iter(|| calculate_rep_levels(&nested, value_count).unwrap());
    }
}
