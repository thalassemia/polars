use super::super::pages::Nested;
use super::rep::{StackState, num_values};
use super::to_length;

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
        let mut validity_bonus = 0;
        for curr_nested in nested {
            match curr_nested {
                Nested::Primitive(validity, is_optional, len) => {
                    if let Some(validity) = validity {
                        stack.push(
                            StackState {
                                current_level,
                                validity_bonus,
                                lengths: Box::new(std::iter::empty()),
                                validity: Some(validity.iter()),
                                is_primitive: true,
                                is_optional: *is_optional,
                                current_length: *len,
                                total_processed: 0,
                            }
                        );
                    }
                    stack.push(
                        StackState {
                            current_level,
                            validity_bonus,
                            lengths: Box::new(std::iter::empty()),
                            validity: None,
                            is_primitive: true,
                            is_optional: *is_optional,
                            current_length: *len,
                            total_processed: 0,
                        }
                    );
                }
                Nested::List(nested) => {
                    let is_optional = nested.is_optional as usize;
                    let next_level = current_level + is_optional;
                    let next_validity_bonus = validity_bonus + is_optional;
                    let mut length_iter = to_length(&nested.offsets);
                    let current_length = length_iter.next().unwrap_or(0);
                    stack.push(
                        StackState {
                            current_level,
                            validity_bonus,
                            lengths: Box::new(length_iter),
                            validity: None,
                            is_primitive: false,
                            is_optional: nested.is_optional,
                            current_length,
                            total_processed: 0,
                        }
                    );
                    current_level = next_level;
                    validity_bonus = next_validity_bonus;
                }
                Nested::LargeList(nested) => {
                    let is_optional = nested.is_optional as usize;
                    let next_level = current_level + is_optional;
                    let next_validity_bonus = validity_bonus + is_optional;
                    let mut length_iter = to_length(&nested.offsets);
                    let current_length = length_iter.next().unwrap_or(0);
                    stack.push(
                        StackState {
                            current_level,
                            validity_bonus,
                            lengths: Box::new(length_iter),
                            validity: None,
                            is_primitive: false,
                            is_optional: nested.is_optional,
                            current_length,
                            total_processed: 0,
                        }
                    );
                    current_level = next_level;
                    validity_bonus = next_validity_bonus;
                }
                Nested::Struct(_, is_optional, len) => {
                    let is_optional_usize = *is_optional as usize;
                    let next_level = current_level + is_optional_usize;
                    let next_validity_bonus = validity_bonus + is_optional_usize;
                    stack.push(
                        StackState {
                            current_level,
                            validity_bonus,
                            lengths: Box::new(std::iter::empty()),
                            validity: None,
                            is_primitive: false,
                            is_optional: *is_optional,
                            current_length: *len,
                            total_processed: 0,
                        }
                    );
                    current_level = next_level;
                    validity_bonus = next_validity_bonus;
                }
                Nested::FixedSizeList {is_optional, width, len, ..} => {
                    let is_optional_usize = *is_optional as usize;
                    let next_level = current_level + is_optional_usize;
                    let next_validity_bonus = validity_bonus + is_optional_usize;
                    let mut length_iter = std::iter::repeat(*width).take(*len);
                    let current_length = length_iter.next().unwrap_or(0);
                    stack.push(
                        StackState {
                            current_level,
                            validity_bonus,
                            lengths: Box::new(length_iter),
                            validity: None,
                            is_primitive: false,
                            is_optional: *is_optional,
                            current_length,
                            total_processed: 0,
                        }
                    );
                    current_level = next_level;
                    validity_bonus = next_validity_bonus;
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
        loop {
            if stack_state.current_length == 0 {
                // No run was consumed yet so this must be a run of length 0
                if stack_state.total_processed == 0 {
                    self.remaining_values -= 1;
                    // Try to advance to next run
                    if let Some(new_length) = stack_state.lengths.next() {
                        stack_state.current_length = new_length;
                    } else {
                        // Make it clear we consumed run of length 0
                        stack_state.total_processed += 1;
                    }
                    return Some(stack_state.current_level as u32)
                }
                if self.stack_idx == 0 {
                    break();
                }
                // Unwind stack if we have consumed a run
                self.stack_idx -= 1;
                stack_state.total_processed = 0;
                stack_state = &mut self.stack[self.stack_idx];
            } else {
                // Run is still going
                break ();
            }
        }
        loop {
            // Advance states to next run after unwinding
            if stack_state.current_length == 0 {
                stack_state.current_length = stack_state.lengths.next().unwrap();
                // If next run also has length 0, return current level
                // and unwind again on next iteration.
                if stack_state.current_length == 0 {
                    self.remaining_values -= 1;
                    stack_state.total_processed += 1;
                    return Some(stack_state.current_level as u32);
                } else {
                    stack_state.total_processed = 0;
                }
            }
            if stack_state.is_primitive {
                // Stack pointer should be at nested level above primitive level
                self.stack_idx -= 1;
                self.remaining_values -= 1;
                if let Some(validity) = &mut stack_state.validity {
                    return Some((stack_state.current_level + stack_state.validity_bonus) as u32 + validity.next().unwrap() as u32);
                } else {
                    return Some((stack_state.current_level + stack_state.validity_bonus) as u32 + stack_state.is_optional as u32)
                }
            }
            stack_state.current_length -= 1;
            stack_state.total_processed += 1;
            self.stack_idx += 1;
            stack_state = &mut self.stack[self.stack_idx];
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
}
