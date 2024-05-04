use arrow::bitmap::utils::BitmapIter;
use arrow::bitmap::Bitmap;

use super::super::pages::Nested;
use super::to_length;

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

/// Store information about recursive stack
#[derive(Debug)]
pub struct StackState<'a> {
    // current repetition level
    pub current_level: usize,
    // add to current level to get definition level
    pub validity_bonus: usize,
    // iterator over lengths of inner values
    pub lengths: Box<dyn DebugIter + 'a>,
    // validity iterator (only for leaf primitive arrays)
    pub validity: Option<BitmapIter<'a>>,
    pub is_primitive: bool,
    pub is_optional: bool,
    // remaining length of current inner value
    pub current_length: usize,
    // total inner values processed
    pub total_processed: usize,
}

/// Iterator adapter of parquet / dremel repetition levels
#[derive(Debug)]
pub struct RepLevelsIter<'a> {
    // current stack for recursion
    stack: Vec<StackState<'a>>,
    // current location on stack
    stack_idx: usize,
    // the total number of items that this iterator will return
    remaining_values: usize,
}

impl<'a> RepLevelsIter<'a> {
    pub fn new(nested: &'a [Nested]) -> Self {
        let remaining_values = num_values(nested);

        let mut stack = vec![];
        let mut current_level = 0;
        let mut validity_bonus = 0;
        for curr_nested in nested {
            match curr_nested {
                Nested::Primitive(validity, is_optional, len) => {
                    validity_bonus += *is_optional as usize;
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
                    let next_level = current_level + nested.is_optional as usize;
                    validity_bonus += nested.is_optional as usize;
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
                }
                Nested::LargeList(nested) => {
                    let next_level = current_level + nested.is_optional as usize;
                    validity_bonus += nested.is_optional as usize;
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
                }
                Nested::Struct(_, is_optional, len) => {
                    let next_level = current_level + *is_optional as usize;
                    validity_bonus += *is_optional as usize;
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
                }
                Nested::FixedSizeList {is_optional, width, len, ..} => {
                    let next_level = current_level + *is_optional as usize;
                    validity_bonus += *is_optional as usize;
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

impl<'a> Iterator for RepLevelsIter<'a> {
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
        // TODO: Figure out how to return 0 only for very first iteration
        // All subsequent iterations should return outer_level + 1
        let outer_level = stack_state.current_level as u32;
        loop {
            // Advance states to next run after unwinding
            if stack_state.current_length == 0 {
                stack_state.current_length = stack_state.lengths.next().unwrap();
                // If next run also has length 0, return current level
                // and unwind again on next iteration.
                if stack_state.current_length == 0 {
                    self.remaining_values -= 1;
                    stack_state.total_processed += 1;
                    return Some(outer_level as u32);
                } else {
                    stack_state.total_processed = 0;
                }
            }
            if stack_state.is_primitive {
                // Stack pointer should be at nested level above primitive level
                self.stack_idx -= 1;
                self.remaining_values -= 1;
                let mut rep_level = stack_state.current_level as u32;
                stack_state = &mut self.stack[self.stack_idx];
                // First element has repetition level equal to outer level
                if stack_state.total_processed == 1 {
                    rep_level = outer_level;
                }
                return Some(rep_level);
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
    use super::super::super::pages::ListNested;
    use super::*;

    fn test(nested: Vec<Nested>, expected: Vec<u32>) {
        let mut iter = RepLevelsIter::new(&nested);
        assert_eq!(iter.size_hint().0, expected.len());
        assert_eq!(iter.by_ref().collect::<Vec<_>>(), expected);
        assert_eq!(iter.size_hint().0, 0);
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
                is_optional: false,
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
                is_optional: false,
                offsets: vec![0, 2, 2, 4].try_into().unwrap(),
                validity: None,
            }),
            Nested::List(ListNested {
                is_optional: false,
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
                is_optional: false,
                offsets: vec![0, 1, 1, 3, 5, 5, 8, 8, 9].try_into().unwrap(),
                validity: None,
            }),
            Nested::List(ListNested {
                is_optional: false,
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
                offsets: vec![0, 1, 2, 3, 3, 4, 4, 4, 4, 5, 6, 8].try_into().unwrap(),
                validity: None,
            }),
            Nested::Primitive(None, true, 8),
        ];
        let expected = vec![0, 1, 0, 0, 1, 1, 0, 1, 1, 0, 0, 1, 1, 2, 0];

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
}
