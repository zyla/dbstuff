use crate::datum;
use crate::hexdump::pretty_hex;
use datum::{Datum, NotNull, Nullability, Nullable, Type};
use proptest::prelude::*;

#[test]
fn serialize_string() {
    let mut w = datum::serialize::Writer::new();
    Datum::String("Hello world! ąść".into()).serialize(&mut w, NotNull);
    assert_snapshot!(pretty_hex(&w.data()), @r###"
    0000:   13 00 00 00  48 65 6c 6c  6f 20 77 6f  72 6c 64 21   ....Hello world!
    0010:   20 c4 85 c5  9b c4 87                                 ......
    "###);
}

#[test]
fn serialize_string_nullable() {
    let mut w = datum::serialize::Writer::new();
    Datum::String("Hello world! ąść".into()).serialize(&mut w, Nullable);
    assert_snapshot!(pretty_hex(&w.data()), @r###"
    0000:   01 13 00 00  00 48 65 6c  6c 6f 20 77  6f 72 6c 64   .....Hello world
    0010:   21 20 c4 85  c5 9b c4 87                             ! ......
    "###);
}

#[test]
fn serialize_int() {
    let mut w = datum::serialize::Writer::new();
    Datum::Int8(0x1234567890abcdef).serialize(&mut w, NotNull);
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   ef cd ab 90  78 56 34 12                             ....xV4.");
}

#[test]
fn serialize_bool() {
    let mut w = datum::serialize::Writer::new();
    Datum::Bool(true).serialize(&mut w, NotNull);
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   01                                                   .");
}

#[test]
fn serialize_null() {
    let mut w = datum::serialize::Writer::new();
    Datum::Null.serialize(&mut w, Nullable);
    assert_snapshot!(pretty_hex(&w.data()), @"0000:   00                                                   .");
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 1000, ..ProptestConfig::default()
    })]
    #[test]
    fn roundtrip_one(d: Datum, n0: Nullability) {
        let n = if d == Datum::Null { Nullable } else { n0 };
        let mut w = datum::serialize::Writer::new();
        d.serialize(&mut w, n);
        let d2 = Datum::deserialize(
            &mut datum::serialize::Reader::new(w.data()),
            n,
            d.ty().unwrap_or(Type::Bool),
        );
        prop_assert_eq!(&d, &d2, "n={:?}, ty={:?}", n, d.ty());
    }

    #[test]
    fn roundtrip_many(tuple: Vec<(Datum, Nullability)>) {
        let mut w = datum::serialize::Writer::new();
        for (d, n0) in &tuple {
            let n = if *d == Datum::Null { Nullable } else { *n0 };
            d.serialize(&mut w, n);
        }
        let mut reader = datum::serialize::Reader::new(w.data());
        let tuple2 = tuple
            .iter()
            .map(|(d, n0)| {
                let n = if *d == Datum::Null { Nullable } else { *n0 };
                (
                    Datum::deserialize(&mut reader, n, d.ty().unwrap_or(Type::Bool)),
                    *n0,
                )
            })
            .collect::<Vec<_>>();
        prop_assert_eq!(&tuple, &tuple2);
    }
}
