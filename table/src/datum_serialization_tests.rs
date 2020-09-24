use crate::hexdump::pretty_hex;
use crate::datum;
use datum::{Datum, Type, NType};

#[test]
fn serialize_string() {
    let mut w = datum::serialize::Writer::new();
    Datum::String("Hello world! ąść".into()).serialize(&mut w, NType::not_null(Type::String));
    assert_snapshot!(pretty_hex(&w.data()), @r###"
    0000:   13 00 00 00  48 65 6c 6c  6f 20 77 6f  72 6c 64 21   ....Hello world!
    0010:   20 c4 85 c5  9b c4 87                                 ......
    "###);
}

#[test]
fn serialize_string_nullable() {
    let mut w = datum::serialize::Writer::new();
    Datum::String("Hello world! ąść".into()).serialize(&mut w, NType::nullable(Type::String));
    assert_snapshot!(pretty_hex(&w.data()), @r###"
    0000:   01 13 00 00  00 48 65 6c  6c 6f 20 77  6f 72 6c 64   .....Hello world
    0010:   21 20 c4 85  c5 9b c4 87                             ! ......
    "###);
}

#[test]
fn serialize_int() {
    let mut w = datum::serialize::Writer::new();
    Datum::Int8(0x1234567890abcdef).serialize(&mut w, NType::not_null(Type::Int8));
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   ef cd ab 90  78 56 34 12                             ....xV4.");
}

#[test]
fn serialize_bool() {
    let mut w = datum::serialize::Writer::new();
    Datum::Bool(true).serialize(&mut w, NType::not_null(Type::Bool));
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   01                                                   .");
}

#[test]
fn serialize_null() {
    let mut w = datum::serialize::Writer::new();
    Datum::Null.serialize(&mut w, NType::nullable(Type::Int8));
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   00                                                   .");
}
