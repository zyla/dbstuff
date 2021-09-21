use crate::hexdump::pretty_hex;
use crate::page;
use crate::page::{PageFull, TupleBlockPage};
use buffer_pool::disk_manager::PAGE_SIZE;

#[derive(Debug, Copy, Clone)]
struct Metadata {
    foo: u64,
    bar: u64,
    baz: u64,
}

const EXAMPLE_METADATA: Metadata = Metadata {
    foo: 0x0102030405060708,
    bar: 1,
    baz: 1,
};

#[test]
fn test_new_page() {
    let mut page_data = [0u8; PAGE_SIZE];
    TupleBlockPage::new(&mut page_data, &EXAMPLE_METADATA);
    assert_snapshot!(pretty_hex(&&page_data[..]), @r###"
    0000:   00 00 00 00  18 00 e8 0f  00 00 00 00  00 00 00 00   ................
    0010:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    0fe0:   00 00 00 00  00 00 00 00  08 07 06 05  04 03 02 01   ................
    0ff0:   01 00 00 00  00 00 00 00  01 00 00 00  00 00 00 00   ................
    "###);
}

#[test]
fn test_insert_tuple() -> page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TupleBlockPage::new(&mut page_data, &EXAMPLE_METADATA);
    let slot1 = page.insert_tuple(b"Hello World")?;
    let slot2 = page.insert_tuple(b"Very very very long tuple")?;
    let slot3 = page.insert_tuple(b"Small")?;
    assert_snapshot!(pretty_hex(&&page.data()[..]), @r###"
    0000:   00 00 00 00  18 00 bf 0f  03 00 00 00  dd 0f 0b 00   ................
    0010:   c4 0f 19 00  bf 0f 05 00  00 00 00 00  00 00 00 00   ................
    0020:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    0fb0:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 53   ...............S
    0fc0:   6d 61 6c 6c  56 65 72 79  20 76 65 72  79 20 76 65   mallVery very ve
    0fd0:   72 79 20 6c  6f 6e 67 20  74 75 70 6c  65 48 65 6c   ry long tupleHel
    0fe0:   6c 6f 20 57  6f 72 6c 64  08 07 06 05  04 03 02 01   lo World........
    0ff0:   01 00 00 00  00 00 00 00  01 00 00 00  00 00 00 00   ................
    "###);

    assert_eq!(page.get_tuple(slot1).unwrap(), b"Hello World");
    assert_eq!(page.get_tuple(slot2).unwrap(), b"Very very very long tuple");
    assert_eq!(page.get_tuple(slot3).unwrap(), b"Small");

    Ok(())
}

#[test]
fn test_page_full() -> page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TupleBlockPage::new(&mut page_data, &EXAMPLE_METADATA);
    page.insert_tuple(&[1u8; PAGE_SIZE - 100])?;
    assert_eq!(page.insert_tuple(&[1u8; 500]), Err(PageFull));

    Ok(())
}

#[test]
fn test_insert_tuple_at() -> page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TupleBlockPage::new(&mut page_data, &EXAMPLE_METADATA);
    page.insert_tuple_at(0, b"A")?;
    page.insert_tuple_at(1, b"B")?;
    page.insert_tuple_at(2, b"C")?;
    page.insert_tuple_at(3, b"D")?;

    assert_eq!(
        page.dump_tuples(),
        vec![b"A".to_vec(), b"B".to_vec(), b"C".to_vec(), b"D".to_vec(),]
    );

    page.insert_tuple_at(1, b"X")?;

    assert_eq!(
        page.dump_tuples(),
        vec![
            b"A".to_vec(),
            b"X".to_vec(),
            b"B".to_vec(),
            b"C".to_vec(),
            b"D".to_vec(),
        ]
    );

    Ok(())
}
