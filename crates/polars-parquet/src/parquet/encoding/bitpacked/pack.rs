/// Macro that generates a packing function taking the number of bits as a const generic
mod pack8 {
    use std::ptr::{read_unaligned as load_unaligned, write_unaligned as store_unaligned};

    use crunchy::unroll;
    pub unsafe fn pack<const NUM_BITS: usize>(input_arr: &[u8; 8], output_arr: &mut [u8]) {
        if NUM_BITS == 0 {
            for out in output_arr {
                *out = 0;
            }
            return;
        }
        assert!(NUM_BITS <= 8);
        assert!(output_arr.len() >= NUM_BITS);

        let input_ptr = input_arr.as_ptr();
        let mut output_ptr = output_arr.as_mut_ptr();
        let mut out_register: u8 = load_unaligned(input_ptr);
        
        unroll! {
            for iter in 0..6 {
                let i: usize = 1 + iter;
        
                let bits_filled: usize = i * NUM_BITS;
                let inner_cursor: usize = bits_filled % 8;
                let remaining: usize = 8 - inner_cursor;
        
                let offset_ptr = input_ptr.add(i);
                let in_register: u8 = load_unaligned(offset_ptr);

                out_register =
                    if inner_cursor > 0 {
                        out_register | (in_register << inner_cursor)
                    } else {
                        in_register
                    };
        
                if remaining <= NUM_BITS {
                    store_unaligned(output_ptr, out_register);
                    output_ptr = output_ptr.offset(1);
                    if 0 < remaining && remaining < NUM_BITS {
                        out_register = in_register >> remaining
                    }
                }
            }
        }
        let in_register: u8 = load_unaligned(input_ptr.add(7));
        out_register = if 8 - NUM_BITS > 0 {
            out_register | (in_register << (8 - NUM_BITS))
        } else {
            out_register | in_register
        };
        store_unaligned(output_ptr, out_register)
    }
}

/// Pack unpacked `input` into `output` with a bit width of `num_bits`
pub fn pack8(input: &[u8; 8], output: &mut [u8], num_bits: usize) {
    // This will get optimised into a jump table
    seq_macro::seq!(i in 0..9 {
        if i == num_bits {
            unsafe {
                return pack8::pack::<i>(input, output);
            }
        }
    });
    unreachable!("invalid num_bits {}", num_bits);
}

/// Macro that generates a packing function taking the number of bits as a const generic
mod pack16 {
    use std::ptr::{read_unaligned as load_unaligned, write_unaligned as store_unaligned};

    use crunchy::unroll;
    pub unsafe fn pack<const NUM_BITS: usize>(input_arr: &[u16; 16], output_arr: &mut [u8]) {
        if NUM_BITS == 0 {
            for out in output_arr {
                *out = 0;
            }
            return;
        }
        assert!(NUM_BITS <= 16);
        assert!(output_arr.len() >= NUM_BITS * 2);

        let input_ptr = input_arr.as_ptr();
        let mut output_ptr = output_arr.as_mut_ptr() as *mut u16;
        let mut out_register: u16 = load_unaligned(input_ptr);
        
        unroll! {
            for iter in 0..14 {
                let i: usize = 1 + iter;
        
                let bits_filled: usize = i * NUM_BITS;
                let inner_cursor: usize = bits_filled % 16;
                let remaining: usize = 16 - inner_cursor;
        
                let offset_ptr = input_ptr.add(i);
                let in_register: u16 = load_unaligned(offset_ptr);

                out_register =
                    if inner_cursor > 0 {
                        out_register | (in_register << inner_cursor)
                    } else {
                        in_register
                    };
        
                if remaining <= NUM_BITS {
                    store_unaligned(output_ptr, out_register);
                    output_ptr = output_ptr.offset(1);
                    if 0 < remaining && remaining < NUM_BITS {
                        out_register = in_register >> remaining
                    }
                }
            }
        }
        let in_register: u16 = load_unaligned(input_ptr.add(15));
        out_register = if 16 - NUM_BITS > 0 {
            out_register | (in_register << (16 - NUM_BITS))
        } else {
            out_register | in_register
        };
        store_unaligned(output_ptr, out_register)
    }
}

/// Pack unpacked `input` into `output` with a bit width of `num_bits`
pub fn pack16(input: &[u16; 16], output: &mut [u8], num_bits: usize) {
    // This will get optimised into a jump table
    seq_macro::seq!(i in 0..17 {
        if i == num_bits {
            unsafe {
                return pack16::pack::<i>(input, output);
            }
        }
    });
    unreachable!("invalid num_bits {}", num_bits);
}

/// Macro that generates a packing function taking the number of bits as a const generic
mod pack32 {
    use std::ptr::{read_unaligned as load_unaligned, write_unaligned as store_unaligned};

    use crunchy::unroll;
    pub unsafe fn pack<const NUM_BITS: usize>(input_arr: &[u32; 32], output_arr: &mut [u8]) { 
        if NUM_BITS == 0 {
            for out in output_arr {
                *out = 0;
            }
            return;
        }
        assert!(NUM_BITS <= 32);
        assert!(output_arr.len() >= NUM_BITS * 4);

        let input_ptr = input_arr.as_ptr();
        let mut output_ptr = output_arr.as_mut_ptr() as *mut u32;
        let mut out_register: u32 = load_unaligned(input_ptr);
        
        unroll! {
            for iter in 0..30 {
                let i: usize = 1 + iter;
        
                let bits_filled: usize = i * NUM_BITS;
                let inner_cursor: usize = bits_filled % 32;
                let remaining: usize = 32 - inner_cursor;
        
                let offset_ptr = input_ptr.add(i);
                let in_register: u32 = load_unaligned(offset_ptr);

                out_register =
                    if inner_cursor > 0 {
                        out_register | (in_register << inner_cursor)
                    } else {
                        in_register
                    };
        
                if remaining <= NUM_BITS {
                    store_unaligned(output_ptr, out_register);
                    output_ptr = output_ptr.offset(1);
                    if 0 < remaining && remaining < NUM_BITS {
                        out_register = in_register >> remaining
                    }
                }
            }
        }
        let in_register: u32 = load_unaligned(input_ptr.add(31));
        out_register = if (32 - NUM_BITS) > 0 {
            out_register | (in_register << (32 - NUM_BITS))
        } else {
            out_register | in_register
        };
        store_unaligned(output_ptr, out_register)
    }
}

/// Pack unpacked `input` into `output` with a bit width of `num_bits`
pub fn pack32(input: &[u32; 32], output: &mut [u8], num_bits: usize) {
    // This will get optimised into a jump table
    seq_macro::seq!(i in 0..33 {
        if i == num_bits {
            unsafe {
                return pack32::pack::<i>(input, output);
            }
        }
    });
    unreachable!("invalid num_bits {}", num_bits);
}

/// Macro that generates a packing function taking the number of bits as a const generic
mod pack64 {
    use std::ptr::{read_unaligned as load_unaligned, write_unaligned as store_unaligned};

    use crunchy::unroll;
    pub unsafe fn pack<const NUM_BITS: usize>(input_arr: &[u64; 64], output_arr: &mut [u8]) {    
        if NUM_BITS == 0 {
            for out in output_arr {
                *out = 0;
            }
            return;
        }
        assert!(NUM_BITS <= 64);
        assert!(output_arr.len() >= NUM_BITS * 8);

        let input_ptr = input_arr.as_ptr();
        let mut output_ptr = output_arr.as_mut_ptr() as *mut u64;
        let mut out_register: u64 = load_unaligned(input_ptr);
        
        unroll! {
            for iter in 0..62 {
                let i: usize = 1 + iter;
        
                let bits_filled: usize = i * NUM_BITS;
                let inner_cursor: usize = bits_filled % 64;
                let remaining: usize = 64 - inner_cursor;
        
                let offset_ptr = input_ptr.add(i);
                let in_register: u64 = load_unaligned(offset_ptr);

                out_register =
                    if inner_cursor > 0 {
                        out_register | (in_register << inner_cursor)
                    } else {
                        in_register
                    };
        
                if remaining <= NUM_BITS {
                    store_unaligned(output_ptr, out_register);
                    output_ptr = output_ptr.offset(1);
                    if 0 < remaining && remaining < NUM_BITS {
                        out_register = in_register >> remaining
                    }
                }
            }
        }
        let in_register: u64 = load_unaligned(input_ptr.add(63));
        out_register = if 64 - NUM_BITS > 0 {
            out_register | (in_register << (64 - NUM_BITS))
        } else {
            out_register | in_register
        };
        store_unaligned(output_ptr, out_register)
    }
}

/// Pack unpacked `input` into `output` with a bit width of `num_bits`
pub fn pack64(input: &[u64; 64], output: &mut [u8], num_bits: usize) {
    // This will get optimised into a jump table
    seq_macro::seq!(i in 0..65 {
        if i == num_bits {
            unsafe {
                return pack64::pack::<i>(input, output);
            }
        }
    });
    unreachable!("invalid num_bits {}", num_bits);
}

#[cfg(test)]
mod tests {
    use super::super::unpack::*;
    use super::*;

    #[test]
    fn test_basic() {
        let input = [0u16, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        for num_bits in 4..16 {
            let mut output = [0u8; 16 * 2];
            pack16(&input, &mut output, num_bits);
            let mut other = [0u16; 16];
            unpack16(&output, &mut other, num_bits);
            assert_eq!(other, input);
        }
    }

    #[test]
    fn test_u32() {
        let input = [
            0u32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0u32, 1, 2, 3, 4, 5, 6, 7, 8,
            9, 10, 11, 12, 13, 14, 15,
        ];
        for num_bits in 4..32 {
            let mut output = [0u8; 32 * 4];
            pack32(&input, &mut output, num_bits);
            let mut other = [0u32; 32];
            unpack32(&output, &mut other, num_bits);
            assert_eq!(other, input);
        }
    }
}
