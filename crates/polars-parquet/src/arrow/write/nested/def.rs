use arrow::bitmap::utils::BitmapIter;

use super::super::pages::Nested;
use super::rep::{DebugIter, num_values};
use super::to_length;

/// Store information about recursive stack
#[derive(Debug)]
pub struct StackState<'a> {
    // current definition level
    pub current_level: u32,
    // iterator over validities of inner values
    pub validity: Option<BitmapIter<'a>>,
    // add to null level when inner field is defined with 0 length
    pub is_optional: u32,
    // iterator over lengths of inner values
    pub lengths: Box<dyn DebugIter + 'a>,
    // iterator over validities of inner primitive array
    pub primitive_validity: Option<BitmapIter<'a>>,
    // add to current level when inner primitive field defined with no validity
    pub primitive_is_optional: u32,
    // whether next level is primitive array
    pub is_primitive: bool,
    // definition level if field is null
    pub null_level: u32,
    // remaining length of current inner field value
    pub current_length: usize,
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

        // Add root node to stack
        let mut stack = vec![];
        let mut current_level = 0;
        let mut null_level = 0;
        stack.push(
            StackState {
                current_level,
                lengths: Box::new(std::iter::empty()),
                validity: None,
                primitive_validity: None,
                primitive_is_optional: 0,
                is_optional: 0,
                is_primitive: false,
                null_level,
                current_length: nested[0].len(),
            }
        );
        for curr_nested in nested {
            match curr_nested {
                Nested::Primitive(validity, is_optional, _) => {
                    let validity_iter;
                    if let Some(validity) = validity {
                        validity_iter = Some(validity.iter());
                    } else {
                        validity_iter = None;
                    }
                    if let Some(last_stack_item) = stack.last_mut() {
                        last_stack_item.primitive_validity = validity_iter;
                        last_stack_item.primitive_is_optional = *is_optional as u32;
                        last_stack_item.is_primitive = true;
                    } else {
                        unreachable!();
                    }
                }
                Nested::List(nested) => {
                    current_level += nested.is_optional as u32 + 1;
                    let mut length_iter = to_length(&nested.offsets);
                    let current_length = length_iter.next().unwrap_or(0);
                    let validity_iter;
                    if let Some(validity) = &nested.validity {
                        validity_iter = Some(validity.iter());
                    } else {
                        validity_iter = None;
                    }
                    stack.push(
                        StackState {
                            current_level,
                            lengths: Box::new(length_iter),
                            is_primitive: false,
                            validity: validity_iter,
                            primitive_validity: None,
                            primitive_is_optional: 0,
                            is_optional: nested.is_optional as u32,
                            null_level,
                            current_length,
                        }
                    );
                    null_level = current_level;
                }
                Nested::LargeList(nested) => {
                    current_level += nested.is_optional as u32 + 1;
                    let mut length_iter = to_length(&nested.offsets);
                    let current_length = length_iter.next().unwrap_or(0);
                    let validity_iter;
                    if let Some(validity) = &nested.validity {
                        validity_iter = Some(validity.iter());
                    } else {
                        validity_iter = None;
                    }
                    stack.push(
                        StackState {
                            current_level,
                            lengths: Box::new(length_iter),
                            is_primitive: false,
                            validity: validity_iter,
                            primitive_validity: None,
                            primitive_is_optional: 0,
                            is_optional: nested.is_optional as u32,
                            null_level,
                            current_length,
                        }
                    );
                    null_level = current_level;
                }
                Nested::Struct(validity, is_optional, len) => {
                    current_level += *is_optional as u32;
                    let mut length_iter = std::iter::repeat(1).take(*len);
                    let current_length = length_iter.next().unwrap_or(0);
                    let validity_iter;
                    if let Some(validity) = validity {
                        validity_iter = Some(validity.iter());
                    } else {
                        validity_iter = None;
                    }
                    stack.push(
                        StackState {
                            current_level,
                            lengths: Box::new(length_iter),
                            is_primitive: false,
                            validity: validity_iter,
                            primitive_validity: None,
                            primitive_is_optional: 0,
                            is_optional: *is_optional as u32,
                            null_level,
                            current_length,
                        }
                    );
                    null_level = current_level;
                }
                Nested::FixedSizeList {is_optional, width, len, ..} => {
                    current_level += *is_optional as u32 + 1;
                    let mut length_iter = std::iter::repeat(*width).take(*len);
                    let current_length = length_iter.next().unwrap_or(0);
                    stack.push(
                        StackState {
                            current_level,
                            lengths: Box::new(length_iter),
                            is_primitive: false,
                            validity: None,
                            primitive_validity: None,
                            primitive_is_optional: 0,
                            is_optional: *is_optional as u32,
                            null_level,
                            current_length,
                        }
                    );
                    null_level = current_level;
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
         while stack_state.current_length == 0 {
            if self.stack_idx == 0 {
                self.remaining_values -= 1;
                return Some(0);
            }
            // Start next group if current is complete
            stack_state.current_length = stack_state.lengths.next().unwrap_or(0);
            self.stack_idx -= 1;
            stack_state = &mut self.stack[self.stack_idx];
        }
        let mut not_valid_level = 0;
        loop {
            stack_state.current_length -= 1;
            if stack_state.is_primitive {
                self.remaining_values -= 1;
                let is_valid;
                if let Some(validity) = &mut stack_state.primitive_validity {
                    is_valid = validity.next().unwrap() as u32;
                } else {
                    is_valid = stack_state.primitive_is_optional;
                }
                if not_valid_level > 0 {
                    return Some(not_valid_level);
                }
                return Some(stack_state.current_level + is_valid);
            }
            // Advance current group and move deeper into stack
            self.stack_idx += 1;
            stack_state = &mut self.stack[self.stack_idx];
            let is_valid;
            if let Some(validity) = &mut stack_state.validity {
                is_valid = validity.next().unwrap() as u32;
                // After encountering a field that is null, we must still
                // recursively traverse inner fields (decrementing lengths and
                // and advancing validity iterators), but the definition level
                // that is returned is fixed at the level above the null field.
                if is_valid == 0 && not_valid_level == 0 {
                    not_valid_level = stack_state.null_level;
                }
            } else {
                is_valid = stack_state.is_optional;
            }
            if stack_state.current_length == 0 {
                self.remaining_values -= 1;
                if not_valid_level > 0 {
                    return Some(not_valid_level);
                }
                return Some(stack_state.null_level + is_valid);
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
