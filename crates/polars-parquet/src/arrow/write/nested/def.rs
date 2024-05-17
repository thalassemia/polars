use polars_error::PolarsResult;

use super::super::pages::Nested;

/// Constructs iterators for def levels of `array`
pub fn calculate_def_levels(nested: &[Nested], value_count: usize) -> PolarsResult<Vec<u32>> {
    if nested.is_empty() {
        return Ok(vec![]);
    }
    let mut def_levels = Vec::with_capacity(value_count);

    def_levels_recursive(nested, &mut def_levels, 0, 0, nested[0].len())?;
    Ok(def_levels)
}

fn def_levels_recursive(
    nested: &[Nested],
    def_levels: &mut Vec<u32>,
    current_level: u32,
    offset: usize,
    length: usize,
) -> PolarsResult<()> {
    let current_nested = &nested[0];
    match current_nested {
        Nested::Primitive(validity, is_optional, _) => match validity {
            Some(bitmap) => {
                let mut bitmap_sliced = bitmap.clone();
                bitmap_sliced.slice(offset, length);
                def_levels.extend(
                    bitmap_sliced
                        .iter()
                        .zip(std::iter::repeat(current_level))
                        .map(|(is_valid, def_null)| def_null + is_valid as u32)
                        .take(length),
                );
            },
            None => {
                def_levels
                    .extend(std::iter::repeat(current_level + *is_optional as u32).take(length));
            },
        },
        Nested::List(list_nested) => {
            let mut sliced_offsets = list_nested.offsets.clone();
            // Inner values are already sliced so subtract first offset
            let first_offset = *sliced_offsets.first() as usize;
            sliced_offsets.slice(offset, length + 1);
            // Fields inside lists get extra +1 if defined
            let next_level = current_level + list_nested.is_optional as u32 + 1;
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                for (i, is_valid) in sliced_bitmap.iter().enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i);
                        let inner_length = end - start;
                        if inner_length == 0 {
                            // Inner field not defined so no extra +1
                            def_levels.push(next_level - 1);
                        } else {
                            def_levels_recursive(
                                &nested[1..],
                                def_levels,
                                next_level,
                                start - first_offset,
                                inner_length,
                            )?;
                        }
                    } else {
                        def_levels.push(current_level);
                    }
                }
            } else {
                for i in 0..length {
                    let (start, end) = sliced_offsets.start_end(i);
                    let inner_length = end - start;
                    if inner_length == 0 {
                        // Inner field not defined so no extra +1
                        def_levels.push(next_level - 1);
                    } else {
                        def_levels_recursive(
                            &nested[1..],
                            def_levels,
                            next_level,
                            start - first_offset,
                            inner_length,
                        )?;
                    }
                }
            }
        },
        Nested::LargeList(list_nested) => {
            let mut sliced_offsets = list_nested.offsets.clone();
            // Inner values are already sliced so subtract first offset
            let first_offset = *sliced_offsets.first() as usize;
            sliced_offsets.slice(offset, length + 1);
            // Fields inside lists get extra +1 if defined
            let next_level = current_level + list_nested.is_optional as u32 + 1;
            if let Some(bitmap) = &list_nested.validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                for (i, is_valid) in sliced_bitmap.iter().enumerate() {
                    if is_valid {
                        let (start, end) = sliced_offsets.start_end(i);
                        let inner_length = end - start;
                        if inner_length == 0 {
                            // Inner field not defined so no extra +1
                            def_levels.push(next_level - 1);
                        } else {
                            def_levels_recursive(
                                &nested[1..],
                                def_levels,
                                next_level,
                                start - first_offset,
                                inner_length,
                            )?;
                        }
                    } else {
                        def_levels.push(current_level);
                    }
                }
            } else {
                for i in 0..length {
                    let (start, end) = sliced_offsets.start_end(i);
                    let inner_length = end - start;
                    if inner_length == 0 {
                        // Inner field not defined so no extra +1
                        def_levels.push(next_level - 1);
                    } else {
                        def_levels_recursive(
                            &nested[1..],
                            def_levels,
                            next_level,
                            start - first_offset,
                            inner_length,
                        )?;
                    }
                }
            }
        },
        Nested::Struct(validity, is_optional, ..) => {
            let next_level = current_level + *is_optional as u32;
            if let Some(bitmap) = validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                for (i, is_valid) in sliced_bitmap.iter().enumerate() {
                    if is_valid {
                        def_levels_recursive(&nested[1..], def_levels, next_level, offset + i, 1)?;
                    } else {
                        def_levels.push(current_level);
                    }
                }
            } else {
                for i in 0..length {
                    def_levels_recursive(&nested[1..], def_levels, next_level, offset + i, 1)?;
                }
            }
        },
        Nested::FixedSizeList {
            is_optional,
            width,
            validity,
            ..
        } => {
            // Fields inside lists get extra +1 if defined
            let next_level = current_level + *is_optional as u32 + 1;
            if let Some(bitmap) = validity {
                let mut sliced_bitmap = bitmap.clone();
                sliced_bitmap.slice(offset, length);
                for (i, is_valid) in sliced_bitmap.iter().enumerate() {
                    if is_valid {
                        // width > 0 so no need to consider that case
                        def_levels_recursive(
                            &nested[1..],
                            def_levels,
                            next_level,
                            width * i,
                            *width,
                        )?;
                    } else {
                        def_levels.push(current_level);
                    }
                }
            } else {
                for i in 0..length {
                    def_levels_recursive(&nested[1..], def_levels, next_level, width * i, *width)?;
                }
            }
        },
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::super::pages::ListNested;
    use super::super::rep::num_values;
    use super::*;

    use test::Bencher;
    use arrow::bitmap::Bitmap;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let value_count = num_values(&nested);
        if let Ok(result) = calculate_def_levels(&nested, value_count) {
            assert_eq!(result.len(), expected.len());
            assert_eq!(result, expected);
        } else {
            panic!("Failed to calculate def levels.")
        }
    }

    #[test]
    fn struct_optional() {
        let b = [
            true, false, true, true, false, true, false, false, true, true,
        ];
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(Some(b.into()), true, 10),
        ];
        let expected = vec![2, 1, 2, 2, 1, 2, 1, 1, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn nested_edge_simple() {
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 2),
        ];
        let expected = vec![3, 3];

        test(nested, expected)
    }

    #[test]
    fn struct_optional_1() {
        let b = [
            true, false, true, true, false, true, false, false, true, true,
        ];
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(Some(b.into()), true, 10),
        ];
        let expected = vec![2, 1, 2, 2, 1, 2, 1, 1, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn struct_optional_optional() {
        let nested = vec![
            Nested::Struct(None, true, 10),
            Nested::Primitive(None, true, 10),
        ];
        let expected = vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn l1_required_required() {
        let nested = vec![
            // [[0, 1], [], [2, 0, 3], [4, 5, 6], [], [7, 8, 9], [], [10]]
            Nested::List(ListNested {
                is_optional: false,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 12),
        ];
        let expected = vec![1, 1, 0, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 0, 1];

        test(nested, expected)
    }

    #[test]
    fn l1_optional_optional() {
        // [[0, 1], None, [2, None, 3], [4, 5, 6], [], [7, 8, 9], None, [10]]

        let v0 = [true, false, true, true, true, true, false, true];
        let v1 = [
            true, true, //[0, 1]
            true, false, true, //[2, None, 3]
            true, true, true, //[4, 5, 6]
            true, true, true, //[7, 8, 9]
            true, //[10]
        ];
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: Some(v0.into()),
            }),
            Nested::Primitive(Some(v1.into()), true, 12),
        ];
        let expected = vec![3u32, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3];

        test(nested, expected)
    }

    #[test]
    fn l2_required_required_required() {
        /*
        [
            [
                [1,2,3],
                [4,5,6,7],
            ],
            [
                [8],
                [9, 10]
            ]
        ]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: false,
                offsets: vec![0, 2, 4].try_into().unwrap(),
                validity: None,
            }),
            Nested::List(ListNested {
                is_optional: false,
                offsets: vec![0, 3, 7, 8, 10].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 10),
        ];
        let expected = vec![2, 2, 2, 2, 2, 2, 2, 2, 2, 2];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_required_required() {
        let a = [true, false, true, true];
        /*
        [
            [
                [1,2,3],
                [4,5,6,7],
            ],
            None,
            [
                [8],
                [],
                [9, 10]
            ]
        ]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 2, 5].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::List(ListNested {
                is_optional: false,
                offsets: vec![0, 3, 7, 8, 8, 10].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, false, 10),
        ];
        let expected = vec![3, 3, 3, 3, 3, 3, 3, 0, 1, 3, 2, 3, 3];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_optional_required() {
        let a = [true, false, true];
        let b = [true, true, true, true, false];
        /*
        [
            [
                [1,2,3],
                [4,5,6,7],
            ],
            None,
            [
                [8],
                [],
                None,
            ],
        ]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 3, 7, 8, 8, 8].try_into().unwrap(),
                validity: Some(b.into()),
            }),
            Nested::Primitive(None, false, 8),
        ];
        let expected = vec![4, 4, 4, 4, 4, 4, 4, 0, 4, 3, 2];

        test(nested, expected)
    }

    #[test]
    fn l2_optional_optional_optional() {
        let a = [true, false, true];
        let b = [true, true, true, false];
        let c = [true, true, true, true, false, true, true, true];
        /*
        [
            [
                [1,2,3],
                [4,None,6,7],
            ],
            None,
            [
                [8],
                None,
            ],
        ]
        */
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 4].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 3, 7, 8, 8].try_into().unwrap(),
                validity: Some(b.into()),
            }),
            Nested::Primitive(Some(c.into()), true, 8),
        ];
        let expected = vec![5, 5, 5, 5, 4, 5, 5, 0, 5, 2];

        test(nested, expected)
    }

    /*
        [{"a": "a"}, {"a": "b"}],
        None,
        [{"a": "b"}, None, {"a": "b"}],
        [{"a": None}, {"a": None}, {"a": None}],
        [],
        [{"a": "d"}, {"a": "d"}, {"a": "d"}],
        None,
        [{"a": "e"}],
    */
    #[test]
    fn nested_list_struct_nullable() {
        let a = [
            true, true, true, false, true, false, false, false, true, true, true, true,
        ];
        let b = [
            true, true, true, false, true, true, true, true, true, true, true, true,
        ];
        let c = [true, false, true, true, true, true, false, true];
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: Some(c.into()),
            }),
            Nested::Struct(Some(b.into()), true, 12),
            Nested::Primitive(Some(a.into()), true, 12),
        ];
        let expected = vec![4, 4, 0, 4, 2, 4, 3, 3, 3, 1, 4, 4, 4, 0, 4];

        test(nested, expected)
    }

    #[test]
    fn nested_list_struct_nullable1() {
        let c = [true, false];
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 1].try_into().unwrap(),
                validity: Some(c.into()),
            }),
            Nested::Struct(None, true, 1),
            Nested::Primitive(None, true, 1),
        ];
        let expected = vec![4, 0];

        test(nested, expected)
    }

    #[test]
    fn nested_struct_list_nullable() {
        let a = [
            true, false, true, true, true, true, false, true, true, true, true, false,
        ];
        let b = [
            true, true, true, false, true, true, true, true, true, true, true, true, true, true,
            false, true, false, false,
        ];
        let nested = vec![
            Nested::Struct(None, true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12, 13, 15, 18, 18]
                    .try_into()
                    .unwrap(),
                validity: Some(a.into()),
            }),
            Nested::Primitive(Some(b.into()), true, 18),
        ];
        let expected = vec![
            4, 4, 1, 4, 3, 4, 4, 4, 4, 2, 4, 4, 4, 1, 4, 4, 4, 3, 4, 3, 3, 1,
        ];

        test(nested, expected)
    }

    #[test]
    fn nested_struct_list_nullable1() {
        let a = [true, true, false];
        let nested = vec![
            Nested::Struct(None, true, 3),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 1, 1].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::Primitive(None, true, 1),
        ];
        let expected = vec![4, 2, 1];

        test(nested, expected)
    }

    #[test]
    fn nested_list_struct_list_nullable1() {
        /*
        [
            [{"a": ["b"]}, None],
        ]
        */

        let a = [true];
        let b = [true, false];
        let c = [true, false];
        let d = [true];
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::Struct(Some(b.into()), true, 2),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 1].try_into().unwrap(),
                validity: Some(c.into()),
            }),
            Nested::Primitive(Some(d.into()), true, 1),
        ];
        /*
                0 6
                1 6
                0 0
                0 6
                1 2
        */
        let expected = vec![6, 2];

        test(nested, expected)
    }

    #[test]
    fn nested_list_struct_list_nullable() {
        /*
            [
            [{"a": ["a"]}, {"a": ["b"]}],
            None,
            [{"a": ["b"]}, None, {"a": ["b"]}],
            [{"a": None}, {"a": None}, {"a": None}],
            [],
            [{"a": ["d"]}, {"a": [None]}, {"a": ["c", "d"]}],
            None,
            [{"a": []}],
        ]
            */
        let a = [true, false, true, true, true, true, false, true];
        let b = [
            true, true, true, false, true, true, true, true, true, true, true, true,
        ];
        let c = [
            true, true, true, false, true, false, false, false, true, true, true, true,
        ];
        let d = [true, true, true, true, true, false, true, true];
        let nested = vec![
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::Struct(Some(b.into()), true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 1, 2, 3, 3, 4, 4, 4, 4, 5, 6, 8, 8]
                    .try_into()
                    .unwrap(),
                validity: Some(c.into()),
            }),
            Nested::Primitive(Some(d.into()), true, 8),
        ];
        let expected = vec![6, 6, 0, 6, 2, 6, 3, 3, 3, 1, 6, 5, 6, 6, 0, 4];

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
        b.iter(|| calculate_def_levels(&nested, value_count).unwrap());
    }
}
