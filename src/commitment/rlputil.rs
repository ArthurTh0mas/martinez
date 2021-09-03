use crate::zeroless_view;
use arrayvec::ArrayVec;
use bytes::BufMut;
use ethnum::U256;
use num_traits::PrimInt;
use std::{borrow::Borrow, io::Write};

// generateRlpPrefixLenDouble calculates the length of RLP prefix to encode a string of bytes of length l "twice",
// meaning that it is the prefix for rlp(rlp(data))
pub(crate) fn generate_rlp_prefix_len_double(l: usize, first_byte: u8) -> usize {
    if l < 2 {
        // first_byte only matters when there is 1 byte to encode
        if first_byte >= 0x80 {
            2
        } else {
            0
        }
    } else if l < 55 {
        2
    } else if l < 56 {
        // 2 + 1
        3
    } else if l < 254 {
        4
    } else if l < 256 {
        5
    } else if l < 65533 {
        6
    } else if l < 65536 {
        7
    } else {
        8
    }
}

pub(crate) fn multi_byte_header_prefix_of_len(l: usize) -> u8 {
    // > If a string is more than 55 bytes long, the
    // > RLP encoding consists of a single byte with value 0xB7 plus the length
    // > of the length of the string in binary form, followed by the length of
    // > the string, followed by the string. For example, a length-1024 string
    // > would be encoded as 0xB90400 followed by the string. The range of
    // > the first byte is thus [0xB8, 0xBF].
    //
    // see package rlp/decode.go:887

    0xB7 + l as u8
}

fn generate_byte_array_len(l: usize) -> ArrayVec<u8, 8> {
    let mut buffer = ArrayVec::new();
    if l < 56 {
        buffer.push(0x80 + l as u8);
    } else if l < 256 {
        // len(vn) can be encoded as 1 byte
        buffer.push(multi_byte_header_prefix_of_len(1));
        buffer.push(l as u8);
    } else if l < 65536 {
        // len(vn) is encoded as two bytes
        buffer.push(multi_byte_header_prefix_of_len(2));
        buffer.push((l >> 8) as u8);
        buffer.push((l & 255) as u8);
    } else {
        // len(vn) is encoded as three bytes
        buffer.push(multi_byte_header_prefix_of_len(3));
        buffer.push((l >> 16) as u8);
        buffer.push(((l >> 8) & 255) as u8);
        buffer.push((l & 255) as u8);
    }
    buffer
}

fn generate_byte_array_len_double(l: usize) -> ArrayVec<u8, 8> {
    let mut buffer = ArrayVec::new();
    if l < 55 {
        // After first wrapping, the length will be l + 1 < 56
        buffer.push((0x80 + l + 1) as u8);
        buffer.push((0x80 + l) as u8);
    } else if l < 56 {
        buffer.push(multi_byte_header_prefix_of_len(1));
        buffer.push((l + 1) as u8);
        buffer.push((0x80 + l) as u8);
    } else if l < 254 {
        // after first wrapping, the length will be l + 2 < 256
        buffer.push(multi_byte_header_prefix_of_len(1));
        buffer.push((l + 2) as u8);
        buffer.push(multi_byte_header_prefix_of_len(1));
        buffer.push(l as u8);
    } else if l < 256 {
        // first wrapping is 2 bytes, second wrapping 3 bytes
        buffer.push(multi_byte_header_prefix_of_len(2));
        buffer.push(((l + 2) >> 8) as u8);
        buffer.push(((l + 2) & 255) as u8);
        buffer.push(multi_byte_header_prefix_of_len(1));
        buffer.push(l as u8);
    } else if l < 65533 {
        // both wrappings are 3 bytes
        buffer.push(multi_byte_header_prefix_of_len(2));
        buffer.push(((l + 3) >> 8) as u8);
        buffer.push(((l + 3) & 255) as u8);
        buffer.push(multi_byte_header_prefix_of_len(2));
        buffer.push((l >> 8) as u8);
        buffer.push((l & 255) as u8);
    } else if l < 65536 {
        // first wrapping is 3 bytes, second wrapping is 4 bytes
        buffer.push(multi_byte_header_prefix_of_len(3));
        buffer.push(((l + 3) >> 16) as u8);
        buffer.push((((l + 3) >> 8) & 255) as u8);
        buffer.push(((l + 3) & 255) as u8);
        buffer.push(multi_byte_header_prefix_of_len(2));
        buffer.push(((l >> 8) & 255) as u8);
        buffer.push((l & 255) as u8);
    } else {
        // both wrappings are 4 bytes
        buffer.push(multi_byte_header_prefix_of_len(3));
        buffer.push(((l + 4) >> 16) as u8);
        buffer.push((((l + 4) >> 8) & 255) as u8);
        buffer.push(((l + 4) & 255) as u8);
        buffer.push(multi_byte_header_prefix_of_len(3));
        buffer.push((l >> 16) as u8);
        buffer.push(((l >> 8) & 255) as u8);
        buffer.push((l & 255) as u8);
    }
    buffer
}

fn generate_rlp_prefix_len(l: usize) -> usize {
    if l < 2 {
        0
    } else if l < 56 {
        1
    } else if l < 256 {
        2
    } else if l < 65536 {
        3
    } else {
        4
    }
}

// // RlpSerializable is a value that can be double-RLP coded.
pub trait RlpSerializable {
    fn to_double_rlp<W: Write>(&self, w: &mut W);
    fn double_rlp_len(&self) -> usize;
}

pub struct RlpSerializableBytes<'a>(pub &'a [u8]);

impl<'a> RlpSerializable for RlpSerializableBytes<'a> {
    fn to_double_rlp<W: Write>(&self, w: &mut W) {
        encode_bytes_as_rlp_to_writer(self.0, w, generate_byte_array_len_double)
    }
    fn double_rlp_len(&self) -> usize {
        if let Some(&first_byte) = self.0.get(0) {
            generate_rlp_prefix_len_double(self.0.len(), first_byte) + self.0.len()
        } else {
            0
        }
    }
}

pub struct RlpEncodableBytes<'a>(pub &'a [u8]);

impl<'a> RlpSerializable for RlpEncodableBytes<'a> {
    fn to_double_rlp<W: Write>(&self, w: &mut W) {
        encode_bytes_as_rlp_to_writer(self.0, w, generate_byte_array_len)
    }

    fn double_rlp_len(&self) -> usize {
        generate_rlp_prefix_len(self.0.len()) + self.0.len()
    }
}

fn encode_bytes_as_rlp_to_writer(
    source: &[u8],
    w: &mut impl Write,
    prefix_gen_func: fn(usize) -> ArrayVec<u8, 8>,
) {
    // > 1 byte, write a prefix or prefixes first
    if source.len() > 1 || (source.len() == 1 && source[0] >= 0x80) {
        let prefix_buf = prefix_gen_func(source.len());

        w.write_all(&prefix_buf).unwrap();
    }

    w.write_all(source).unwrap();
}

pub(crate) fn generate_struct_len(l: usize) -> ArrayVec<u8, 4> {
    let mut buffer = ArrayVec::new();
    if l < 56 {
        buffer.push(192 + l as u8);
    } else if l < 256 {
        // l can be encoded as 1 byte
        buffer.push(247 + 1);
        buffer.push(l as u8);
    } else if l < 65536 {
        buffer.push(247 + 2);
        buffer.push((l >> 8) as u8);
        buffer.push((l & 255) as u8);
    } else {
        buffer.push(247 + 3);
        buffer.push((l >> 16) as u8);
        buffer.push(((l >> 8) & 255) as u8);
        buffer.push((l & 255) as u8);
    }
    buffer
}

#[cfg(test)]
mod tests {}
