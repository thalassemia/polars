use super::super::pages::Nested;
use super::rep::num_values;
use super::to_length;

/// Store information about recursive stack
#[derive(Debug)]
pub struct StackState<'a> {
    // Information about nested level
    pub nested: &'a Nested,
    // current definition level
    pub current_level: u32,
    // definition level of level above
    pub parent_level: u32,
    // offset to slice inner level by
    pub offset: usize,
    // number of consecutive values at level
    pub current_length: usize,
    // number of consecutive values processed at level,
    pub processed_length: usize,
}

/// Iterator adapter of parquet / dremel definition levels
#[derive(Debug)]
pub struct DefLevelsIter<'a> {
    // current stack for recursion
    stack: Vec<StackState<'a>>,
    // current location on stack
    stack_idx: usize,
    // the total number of items that this iterator will return
    remaining_values: usize,
}

impl<'a> DefLevelsIter<'a> {
    pub fn new(nested: &'a [Nested]) -> Self {
        let remaining_values = num_values(nested);
        let mut stack = vec![];
        let mut current_level = 0;
        let mut current_length = nested[0].len();
        let mut parent_level = 0;
        for curr_nested in nested {
            stack.push(
                StackState {
                    nested: curr_nested,
                    current_level,
                    parent_level,
                    offset: 0,
                    current_length,
                    processed_length: 0,
                }
            );
            match curr_nested {
                Nested::Primitive(_, _, _) => (),
                Nested::List(nested) => {
                    parent_level = current_level;
                    current_level += nested.is_optional as u32 + 1;
                    current_length = to_length(&nested.offsets).next().unwrap_or(0);
                }
                Nested::LargeList(nested) =>{
                    parent_level = current_level;
                    current_level += nested.is_optional as u32 + 1;
                    current_length = to_length(&nested.offsets).next().unwrap_or(0);
                }
                Nested::FixedSizeList {is_optional, width, len, ..} => {
                    parent_level = current_level;
                    current_level += *is_optional as u32 + 1;
                    current_length = if *len > 0 { *width } else { 0 };
                }
                Nested::Struct(_, is_optional, _) => {
                    parent_level = current_level;
                    current_level += *is_optional as u32;
                    current_length = 1
                }
            };
        }

        Self {
            stack,
            stack_idx: 0,
            remaining_values,
        }
    }
}

impl<'a> Iterator for DefLevelsIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_values == 0 {
            return None;
        }
        let mut stack_state = &mut self.stack[self.stack_idx];
        // Unwind stack until reaching an unfinished group
        while stack_state.current_length == stack_state.processed_length {
            stack_state.processed_length = 0;
            self.stack_idx -= 1;
            stack_state = &mut self.stack[self.stack_idx];
        }
        self.remaining_values -= 1;
        loop {
            stack_state.processed_length += 1;
            stack_state.offset += 1;
            let inner_offset;
            let inner_length;
            let optional_bonus;
            match stack_state.nested {
                Nested::Primitive(validity, is_optional, _) => {
                    if let Some(bitmap) = validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            return Some(stack_state.current_level);
                        }
                    }
                    return Some(stack_state.current_level + *is_optional as u32);
                }
                Nested::List(list_nested) => {
                    if let Some(bitmap) = &list_nested.validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            return Some(stack_state.current_level);
                        }
                    }
                    let first_offset = *list_nested.offsets.first() as usize;
                    let (start, end) = list_nested.offsets.start_end(stack_state.offset - 1);
                    inner_offset = start - first_offset;
                    inner_length = end  - start;
                    optional_bonus = list_nested.is_optional as u32;
                }
                Nested::LargeList(list_nested) => {
                    if let Some(bitmap) = &list_nested.validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            return Some(stack_state.current_level);
                        }
                    }
                    let first_offset = *list_nested.offsets.first() as usize;
                    let (start, end) = list_nested.offsets.start_end(stack_state.offset - 1);
                    inner_offset = start - first_offset;
                    inner_length = end  - start;
                    optional_bonus = list_nested.is_optional as u32;
                }
                Nested::Struct(validity, is_optional, ..) => {
                    if let Some(bitmap) = validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            return Some(stack_state.current_level);
                        }
                    }
                    inner_offset = stack_state.offset - 1;
                    inner_length = 1;
                    optional_bonus = *is_optional as u32;
                }
                Nested::FixedSizeList{ validity, width, is_optional, .. } => {
                    if let Some(bitmap) = validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            return Some(stack_state.current_level);
                        }
                    }
                    inner_offset = *width * (stack_state.processed_length - 1);
                    inner_length = *width;
                    optional_bonus = *is_optional as u32;
                }
            }
            self.stack_idx += 1;
            stack_state = &mut self.stack[self.stack_idx];
            stack_state.offset = inner_offset;
            stack_state.current_length = inner_length;
            if inner_length == 0 {
                return Some(stack_state.parent_level + optional_bonus);
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let length = self.remaining_values;
        (length, Some(length))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::pages::ListNested;

    use test::Bencher;
    use arrow::bitmap::Bitmap;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let mut iter = DefLevelsIter::new(&nested);
        assert_eq!(iter.size_hint().0, expected.len());
        let result = iter.by_ref().collect::<Vec<_>>();
        assert_eq!(result, expected);
        assert_eq!(iter.size_hint().0, 0);
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
        let a = [true, false, true, true, true, true, false, true];
        let b = [
            true, true, true, false, true, true, true, true, true, true, true, true,
        ];
        let nested = vec![
            Nested::Struct(None, true, 12),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![0, 2, 2, 5, 8, 8, 11, 11, 12].try_into().unwrap(),
                validity: Some(a.into()),
            }),
            Nested::Primitive(Some(b.into()), true, 12),
        ];
        let expected = vec![4, 4, 1, 4, 3, 4, 4, 4, 4, 2, 4, 4, 4, 1, 4];

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
                    .take(4 * 1000 + 1)
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
                    .take(6001)
                    .scan(0, |state, &x| { *state += x; Some(*state) })
                    .collect::<Vec<i32>>()
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Struct(
                Some(Bitmap::try_new([1u8, 1, 0, 0, 1].into_iter()
                    .cycle()
                    .take(7501)
                    .collect::<Vec<u8>>(), 7501)
                    .unwrap()),
                true, 7500),
            Nested::List(ListNested {
                is_optional: true,
                offsets: vec![1, 1, 0, 0, 1].iter()
                    .cycle()
                    .take(7501)
                    .scan(0, |state, &x| { *state += x; Some(*state) })
                    .collect::<Vec<i32>>()
                    .try_into()
                    .unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 4500),
        ];
        b.iter(|| {
            let mut iter = DefLevelsIter::new(&nested);
            iter.by_ref().collect::<Vec<_>>()
        });
    }
}
