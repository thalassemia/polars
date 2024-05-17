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
    // Information about nested level
    pub nested: &'a Nested,
    // current repetition level
    pub current_level: u32,
    // offset to slice inner level by
    pub offset: usize,
    // number of consecutive values at level
    pub current_length: usize,
    // number of consecutive values processed at level,
    pub processed_length: usize,
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
        let mut current_length = nested[0].len();
        for curr_nested in nested {
            stack.push(
                StackState {
                    nested: curr_nested,
                    current_level,
                    offset: 0,
                    current_length,
                    processed_length: 0,
                }
            );
            match curr_nested {
                Nested::Primitive(_, _, _) => (),
                Nested::List(nested) => {
                    current_level += nested.is_optional as u32;
                    current_length = to_length(&nested.offsets).next().unwrap_or(0);
                }
                Nested::LargeList(nested) =>{
                    current_level += nested.is_optional as u32;
                    current_length = to_length(&nested.offsets).next().unwrap_or(0);
                }
                Nested::FixedSizeList {is_optional, width, len, ..} => {
                    current_level += *is_optional as u32;
                    current_length = if *len > 0 { *width } else { 0 };
                }
                Nested::Struct(..) => current_length = 1,
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
        // Unwind stack until reaching an unfinished group
        while stack_state.current_length == stack_state.processed_length {
            stack_state.processed_length = 0;
            self.stack_idx -= 1;
            stack_state = &mut self.stack[self.stack_idx];
        }
        // Outer level is most recent ancestor of previous and current value
        let outer_level = stack_state.current_level;
        self.remaining_values -= 1;
        loop {
            stack_state.processed_length += 1;
            stack_state.offset += 1;
            let inner_offset;
            let inner_length;
            match stack_state.nested {
                Nested::Primitive(..) => {
                    if stack_state.processed_length == 1 {
                        return Some(outer_level);
                    }
                    return Some(stack_state.current_level);
                }
                Nested::List(list_nested) => {
                    if let Some(bitmap) = &list_nested.validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            if stack_state.processed_length == 1 {
                                return Some(outer_level);
                            } else {
                                return Some(stack_state.current_level);
                            }
                        }
                    }
                    let first_offset = *list_nested.offsets.first() as usize;
                    let (start, end) = list_nested.offsets.start_end(stack_state.offset - 1);
                    inner_offset = start - first_offset;
                    inner_length = end  - start;
                }
                Nested::LargeList(list_nested) => {
                    if let Some(bitmap) = &list_nested.validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            if stack_state.processed_length == 1 {
                                return Some(outer_level);
                            } else {
                                return Some(stack_state.current_level);
                            }
                        }
                    }
                    let first_offset = *list_nested.offsets.first() as usize;
                    let (start, end) = list_nested.offsets.start_end(stack_state.offset - 1);
                    inner_offset = start - first_offset;
                    inner_length = end  - start;
                }
                Nested::Struct(validity, ..) => {
                    if let Some(bitmap) = validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            if stack_state.processed_length == 1 {
                                return Some(outer_level);
                            } else {
                                return Some(stack_state.current_level);
                            }
                        }
                    }
                    inner_offset = stack_state.offset - 1;
                    inner_length = 1;
                }
                Nested::FixedSizeList{ validity, width, .. } => {
                    if let Some(bitmap) = validity {
                        if !bitmap.get_bit(stack_state.offset - 1) {
                            if stack_state.processed_length == 1 {
                                return Some(outer_level);
                            } else {
                                return Some(stack_state.current_level);
                            }
                        }
                    }
                    inner_offset = *width * (stack_state.processed_length - 1);
                    inner_length = *width;
                }
            }
            self.stack_idx += 1;
            stack_state = &mut self.stack[self.stack_idx];
            stack_state.offset = inner_offset;
            stack_state.current_length = inner_length;
            if inner_length == 0 {
                return Some(outer_level);
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
    use super::super::super::pages::ListNested;
    use super::*;

    use test::Bencher;
    use arrow::bitmap::Bitmap;

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
                offsets: vec![0, 1, 2, 3, 3, 4, 4, 4, 4, 5, 6, 8, 8].try_into().unwrap(),
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
                    .take(7500)
                    .collect::<Vec<u8>>(), 7500)
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
            let mut iter = RepLevelsIter::new(&nested);
            iter.by_ref().collect::<Vec<_>>()
        });
    }
}
