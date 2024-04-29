use std::io::Write;

use super::bitpacked_encode;
use crate::parquet::encoding::{bitpacked, ceil8, uleb128};

// Arbitrary value that balances memory usage and storage overhead
const MAX_VALUES_PER_LITERAL_RUN: usize = (1 << 10) * 8;

// Iterator over an array up to a specified final index
struct ArrayIterator<'a, T> {
    array: &'a [T],
    index: usize,
    final_idx: usize,
}

impl<'a, T: Copy> ArrayIterator<'a, T> {
    fn new(array: &'a [T], final_idx: usize) -> Self {
        ArrayIterator {
            array,
            index: 0,
            final_idx,
        }
    }
}

impl<'a, T: Copy> Iterator for ArrayIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.final_idx {
            self.index += 1;
            Some(self.array[self.index - 1])
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.final_idx - self.index;
        (remaining, Some(remaining))
    }
}

#[allow(clippy::comparison_chain)]
pub fn encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    iterator: I,
    num_bits: u32,
) -> std::io::Result<()> {
    let mut consecutive_repeats: usize = 0;
    let mut buffered_bits = [0; MAX_VALUES_PER_LITERAL_RUN];
    let mut buffer_idx = 0;
    let mut literal_run_idx = 0;
    let mut previous_val = 0;
    for val in iterator {
        if val == previous_val {
            consecutive_repeats += 1;
            // Run is long enough to RLE, no need to buffer values
            if consecutive_repeats > 8 {
                continue;
            // Ensure literal run has multiple of 8 values
            // Take from consecutive repeats if needed to pad up
            } else if consecutive_repeats == 8 {
                let literal_padding = (8 - (literal_run_idx % 8)) % 8;
                consecutive_repeats -= literal_padding;
                literal_run_idx += literal_padding;
            }
            // Too short to RLE, continue to buffer values
        } else if consecutive_repeats > 8 {
            // Flush literal run, if any, before RLE run
            if literal_run_idx > 0 {
                bitpacked_encode_u32(
                    writer,
                    ArrayIterator::new(&buffered_bits, literal_run_idx),
                    num_bits as usize,
                )?;
                literal_run_idx = 0;
            }
            run_length_encode_u32(writer, consecutive_repeats, previous_val, num_bits)?;
            consecutive_repeats = 1;
            buffer_idx = 0;
        } else {
            // Not enough consecutive repeats to RLE, extend literal run
            literal_run_idx = buffer_idx;
            consecutive_repeats = 1;
        }
        // If buffer is full, bit-pack as literal run and reset
        if buffer_idx == MAX_VALUES_PER_LITERAL_RUN {
            bitpacked_encode_u32(
                writer,
                ArrayIterator::new(&buffered_bits, buffer_idx),
                num_bits as usize,
            )?;
            // Consecutive repeats may be consolidated into literal run
            consecutive_repeats -= buffer_idx - literal_run_idx;
            buffer_idx = 0;
            literal_run_idx = 0;
        }
        buffered_bits[buffer_idx] = val;
        previous_val = val;
        buffer_idx += 1;
    }
    // Not enough consecutive repeats to RLE, extend literal run
    if consecutive_repeats <= 8 {
        literal_run_idx = buffer_idx;
        consecutive_repeats = 0;
    }
    if literal_run_idx > 0 {
        bitpacked_encode_u32(
            writer,
            ArrayIterator::new(&buffered_bits, literal_run_idx),
            num_bits as usize,
        )?;
    }
    if consecutive_repeats > 8 {
        run_length_encode_u32(writer, consecutive_repeats, previous_val, num_bits)?;
    }
    Ok(())
}

const U32_BLOCK_LEN: usize = 32;

fn bitpacked_encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    mut iterator: I,
    num_bits: usize,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;

    let chunks = length / U32_BLOCK_LEN;
    let remainder = length - chunks * U32_BLOCK_LEN;
    let mut buffer = [0u32; U32_BLOCK_LEN];

    // simplified from ceil8(U32_BLOCK_LEN * num_bits) since U32_BLOCK_LEN = 32
    let compressed_chunk_size = 4 * num_bits;

    for _ in 0..chunks {
        iterator
            .by_ref()
            .take(U32_BLOCK_LEN)
            .zip(buffer.iter_mut())
            .for_each(|(item, buf)| *buf = item);

        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack::<u32>(&buffer, num_bits, packed.as_mut());
        writer.write_all(&packed[..compressed_chunk_size])?;
    }

    if remainder != 0 {
        // Must be careful here to ensure we write a multiple of `num_bits`
        // (the bit width) to align with the spec. Some readers also rely on
        // this - see https://github.com/pola-rs/polars/pull/13883.

        // this is ceil8(remainder * num_bits), but we ensure the output is a
        // multiple of num_bits by rewriting it as ceil8(remainder) * num_bits
        let compressed_remainder_size = ceil8(remainder) * num_bits;
        iterator
            .by_ref()
            .take(remainder)
            .zip(buffer.iter_mut())
            .for_each(|(item, buf)| *buf = item);

        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack(&buffer[..remainder], num_bits, packed.as_mut());
        writer.write_all(&packed[..compressed_remainder_size])?;
    };
    Ok(())
}

fn run_length_encode_u32<W: Write>(
    writer: &mut W,
    run_length: usize,
    value: u32,
    bit_width: u32,
) -> std::io::Result<()> {
    // write the length + indicator
    let mut header = run_length as u64;
    header <<= 1;
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;

    let num_bytes = ceil8(bit_width as usize);
    let bytes = value.to_le_bytes();
    writer.write_all(&bytes[..num_bytes])?;
    Ok(())
}

#[allow(clippy::comparison_chain)]
pub fn encode_bool<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    iterator: I,
) -> std::io::Result<()> {
    let mut consecutive_repeats: usize = 0;
    let mut buffered_bits = [false; MAX_VALUES_PER_LITERAL_RUN];
    let mut buffer_idx = 0;
    let mut literal_run_idx = 0;
    let mut previous_val = false;
    for bit in iterator {
        if bit == previous_val {
            consecutive_repeats += 1;
            // Run is long enough to RLE, no need to buffer values
            if consecutive_repeats > 8 {
                continue;
            // Ensure literal run has multiple of 8 values
            // Take from consecutive repeats if needed to pad up
            } else if consecutive_repeats == 8 {
                let literal_padding = (8 - (literal_run_idx % 8)) % 8;
                consecutive_repeats -= literal_padding;
                literal_run_idx += literal_padding;
            }
            // Too short to RLE, continue to buffer values
        } else if consecutive_repeats > 8 {
            // Flush literal run, if any, before RLE run
            if literal_run_idx > 0 {
                bitpacked_encode_bool(writer, ArrayIterator::new(&buffered_bits, literal_run_idx))?;
                literal_run_idx = 0;
            }
            run_length_encode_bool(writer, consecutive_repeats, previous_val)?;
            consecutive_repeats = 1;
            buffer_idx = 0;
        } else {
            // Not enough consecutive repeats to RLE, extend literal run
            literal_run_idx = buffer_idx;
            consecutive_repeats = 1;
        }
        // If buffer is full, bit-pack as literal run and reset
        if buffer_idx == MAX_VALUES_PER_LITERAL_RUN {
            bitpacked_encode_bool(writer, ArrayIterator::new(&buffered_bits, buffer_idx))?;
            // Consecutive repeats may be consolidated into literal run
            consecutive_repeats -= buffer_idx - literal_run_idx;
            buffer_idx = 0;
            literal_run_idx = 0;
        }
        buffered_bits[buffer_idx] = bit;
        previous_val = bit;
        buffer_idx += 1;
    }
    // Not enough consecutive repeats to RLE, extend literal run
    if consecutive_repeats <= 8 {
        literal_run_idx = buffer_idx;
        consecutive_repeats = 0;
    }
    if literal_run_idx > 0 {
        bitpacked_encode_bool(writer, ArrayIterator::new(&buffered_bits, literal_run_idx))?;
    }
    if consecutive_repeats > 8 {
        run_length_encode_bool(writer, consecutive_repeats, previous_val)?;
    }
    Ok(())
}

fn bitpacked_encode_bool<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    iterator: I,
) -> std::io::Result<()> {
    // the length of the iterator.
    let length = iterator.size_hint().1.unwrap();

    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;
    bitpacked_encode(writer, iterator)?;
    Ok(())
}

fn run_length_encode_bool<W: Write>(
    writer: &mut W,
    run_length: usize,
    value: bool,
) -> std::io::Result<()> {
    // write the length + indicator
    let mut header = run_length as u64;
    header <<= 1;
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;
    writer.write_all(&(value as u8).to_le_bytes())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::bitmap::BitmapIter;
    use super::*;

    #[test]
    fn bool_basics_1() -> std::io::Result<()> {
        let iter = BitmapIter::new(&[0b10011101u8, 0b10011101], 0, 14);

        let mut vec = vec![];

        encode_bool(&mut vec, iter)?;

        assert_eq!(vec, vec![(2 << 1 | 1), 0b10011101u8, 0b00011101]);

        Ok(())
    }

    #[test]
    fn bool_from_iter() -> std::io::Result<()> {
        let mut vec = vec![];

        encode_bool(
            &mut vec,
            vec![true, true, true, true, true, true, true, true].into_iter(),
        )?;

        assert_eq!(vec, vec![(1 << 1 | 1), 0b11111111]);
        Ok(())
    }

    #[test]
    fn test_encode_u32() -> std::io::Result<()> {
        let mut vec = vec![];

        encode_u32(&mut vec, vec![0, 1, 2, 1, 2, 1, 1, 0, 3].into_iter(), 2)?;

        assert_eq!(
            vec,
            vec![
                (2 << 1 | 1),
                0b01_10_01_00,
                0b00_01_01_10,
                0b_00_00_00_11,
                0b0
            ]
        );
        Ok(())
    }

    #[test]
    fn test_encode_u32_large() -> std::io::Result<()> {
        let mut vec = vec![];

        let values = (0..128).map(|x| x % 4);

        encode_u32(&mut vec, values, 2)?;

        let length = 128;
        let expected = 0b11_10_01_00u8;

        let mut expected = vec![expected; length / 4];
        expected.insert(0, ((length / 8) as u8) << 1 | 1);

        assert_eq!(vec, expected);
        Ok(())
    }

    #[test]
    fn test_u32_other() -> std::io::Result<()> {
        let values = vec![3, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3].into_iter();

        let mut vec = vec![];
        encode_u32(&mut vec, values, 2)?;

        let expected = vec![5, 207, 254, 247, 51];
        assert_eq!(expected, vec);
        Ok(())
    }
}
