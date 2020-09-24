#[derive(Debug, PartialEq, Eq)]
pub enum Type {
    String,
    Int8,
    Bool,
}

#[derive(Debug, PartialEq, Eq)]
pub struct NType {
    pub nullable: bool,
    pub ty: Type,
}

impl NType {
    pub fn nullable(ty: Type) -> Self {
        Self { nullable: true, ty }
    }

    pub fn not_null(ty: Type) -> Self {
        Self { nullable: false, ty }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Datum {
    String(String),
    Int8(i64),
    Bool(bool),
    Null
}

pub mod serialize {
    use super::{Datum, Type, NType};
    use std::{slice, mem};

    pub struct Reader<'a> {
        data: &'a [u8],
        offset: usize
    }

    impl<'a> Reader<'a> {
        pub fn new(data: &'a [u8]) -> Self {
            Self { data, offset: 0 }
        }

        pub fn data(&self) -> &'a [u8] {
            &self.data[self.offset..]
        }

        pub fn advance(&mut self, len: usize) {
            self.offset += len;
        }

        #[allow(clippy::cast_ptr_alignment)]
        unsafe fn read<T: Copy>(&mut self) -> T {
            let len = mem::size_of::<T>();
            let ptr = self.data[self.offset..self.offset+len].as_ptr() as *const T;
            self.advance(len);
            ptr.read_unaligned()
        }

        pub fn read_u32(&mut self) -> u32 {
            unsafe { self.read() }
        }

        pub fn read_u8(&mut self) -> u8 {
            unsafe { self.read() }
        }

        pub fn read_i64(&mut self) -> i64 {
            unsafe { self.read() }
        }

        pub fn read_bytes(&mut self, len: usize) -> &'a [u8] {
            let ptr = &self.data[self.offset..self.offset+len];
            self.advance(len);
            ptr
        }
    }

    pub struct Writer {
        data: Vec<u8>
    }

    impl Writer {
        pub fn new() -> Self {
            Self { data: vec![] }
        }

        pub fn data(&self) -> &[u8] {
            &self.data
        }

        #[allow(clippy::cast_ptr_alignment)]
        unsafe fn write<T: Copy>(&mut self, val: T) {
            let len = mem::size_of::<T>();
            let ptr = &val as *const T as *const u8;
            self.data.extend_from_slice(slice::from_raw_parts(ptr, len));
        }

        pub fn write_u32(&mut self, val: u32) {
            unsafe { self.write(val) }
        }

        pub fn write_i64(&mut self, val: i64) {
            unsafe { self.write(val) }
        }

        pub fn write_u8(&mut self, val: u8) {
            unsafe { self.write(val) }
        }

        pub fn write_bytes(&mut self, b: &[u8]) {
            self.data.extend_from_slice(b);
        }
    }

    impl Datum {
        pub fn deserialize(r: &mut Reader, nty: NType) -> Datum {
            if nty.nullable {
                let is_null = r.read_u8() == 0;
                if is_null {
                    return Datum::Null;
                }
            }
            match nty.ty {
                Type::String => {
                    let len = r.read_u32() as usize;
                    let bytes = r.read_bytes(len);
                    Datum::String(String::from_utf8_lossy(bytes).into_owned())
                },
                Type::Int8 => {
                    Datum::Int8(r.read_i64())
                },
                Type::Bool => {
                    Datum::Bool(r.read_u8() > 0)
                }
            }
        }

        pub fn serialize(&self, w: &mut Writer, nty: NType) {
            const NOT_NULL: u8 = 1;
            match self {
                Datum::String(s) => {
                    if nty.nullable {
                        w.write_u8(NOT_NULL);
                    }
                    w.write_u32(s.len() as u32);
                    w.write_bytes(s.as_bytes());
                },
                Datum::Int8(v) => {
                    if nty.nullable {
                        w.write_u8(NOT_NULL);
                    }
                    w.write_i64(*v);
                }
                Datum::Bool(v) => {
                    if nty.nullable {
                        w.write_u8(NOT_NULL);
                    }
                    w.write_u8(*v as u8);
                }
                Datum::Null => {
                    w.write_u8(0);
                }
            }
        }
    }
}
