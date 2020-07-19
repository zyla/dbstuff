// Modified code from pretty-hex crate. Original license:
//
// MIT License
//
// Copyright (c) 2018 Andrei Volnin
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use core::fmt;
use core::result::Result;

/// Return a multi-line `String` complete with addressing, hex digits, and ASCII representation.
/// Skips multiple consecutive all-zero lines.
pub fn pretty_hex<T: AsRef<[u8]>>(source: &T) -> String {
    let mut writer = String::new();
    pretty_hex_write(&mut writer, source).unwrap_or(());
    writer
}

const COLS: usize = 16;
const CHNK: usize = 4;
const NASCI: char = '.';

/// Dump hex octets grouped in 4-byte words to the writer.
pub fn simple_hex_write<T, W>(writer: &mut W, source: &T) -> Result<(), fmt::Error>
where
    T: AsRef<[u8]>,
    W: fmt::Write,
{
    for (i, x) in source.as_ref().iter().enumerate() {
        write!(
            writer,
            "{:02x}{}",
            x,
            match i + 1 {
                n if n == source.as_ref().len() => "",
                n if n % CHNK == 0 => "  ",
                _ => " ",
            }
        )?;
    }
    Ok(())
}

/// Write multi-line dump complete with addressing, hex digits, and ASCII representation to the writer.
pub fn pretty_hex_write<T, W>(writer: &mut W, source: &T) -> Result<(), fmt::Error>
where
    T: AsRef<[u8]>,
    W: fmt::Write,
{
    let chunks = source.as_ref().chunks(COLS);
    let lines = chunks.len();
    let mut num_zero_lines = 0;
    for (i, row) in chunks.enumerate() {
        let is_zero = row.iter().all(|&c| c == 0);
        if is_zero {
            num_zero_lines += 1;
        } else {
            num_zero_lines = 0;
        }
        if num_zero_lines == 2 {
            writeln!(writer, "*")?;
            continue;
        }
        if num_zero_lines > 2 {
            continue;
        }
        write!(writer, "{:04x}:   ", i * COLS)?;
        simple_hex_write(writer, &row)?;
        let pad = COLS - row.len();
        let pad = pad * 3 + pad / CHNK;
        for _ in 0..pad {
            writer.write_char(' ')?;
        }
        write!(writer, "   ")?;
        for x in row {
            writer.write_char(if x.is_ascii() && !x.is_ascii_control() {
                (*x).into()
            } else {
                NASCI
            })?;
        }
        if i + 1 < lines {
            writeln!(writer, "")?;
        }
    }
    Ok(())
}

/// Reference wrapper for use in arguments formatting.
pub struct Hex<'a, T: 'a>(&'a T);

impl<'a, T: 'a + AsRef<[u8]>> fmt::Display for Hex<'a, T> {
    /// Formats the value by `simple_hex_write` using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        simple_hex_write(f, self.0)
    }
}

impl<'a, T: 'a + AsRef<[u8]>> fmt::Debug for Hex<'a, T> {
    /// Formats the value by `pretty_hex_write` using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        pretty_hex_write(f, self.0)
    }
}

/// Allows generates hex dumps to a formatter.
pub trait PrettyHex: Sized {
    /// Wrap self reference for use in `std::fmt::Display`/`std::fmt::Debug` formating as hex dumps.
    fn hex_dump<'a>(&'a self) -> Hex<'a, Self>;
}

impl PrettyHex for Vec<u8> {
    fn hex_dump<'a>(&'a self) -> Hex<'a, Self> {
        Hex(self)
    }
}

impl<'a> PrettyHex for &'a [u8] {
    fn hex_dump<'b>(&'b self) -> Hex<'b, Self> {
        Hex(self)
    }
}

impl<'a> PrettyHex for &'a mut [u8] {
    fn hex_dump<'b>(&'b self) -> Hex<'b, Self> {
        Hex(self)
    }
}
