use crate::hexdump::pretty_hex;
use crate::table_page;
use crate::table_page::{PageFull, TablePage};
use buffer_pool::disk_manager::PAGE_SIZE;

#[test]
fn test_new_page() {
    let mut page_data = [0u8; PAGE_SIZE];
    TablePage::new(&mut page_data);
    assert_snapshot!(pretty_hex(&&page_data[..]), @r###"
    0000:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0010:   00 10 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0020:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    "###);
}

#[test]
fn test_insert_tuple() -> table_page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut page_data);
    let slot1 = page.insert_tuple(b"Hello World")?;
    let slot2 = page.insert_tuple(b"Very very very long tuple")?;
    let slot3 = page.insert_tuple(b"Small")?;
    assert_snapshot!(pretty_hex(&&page.data()[..]), @r###"
    0000:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0010:   d7 0f 00 00  03 00 00 00  f5 0f 00 00  0b 00 00 00   ................
    0020:   dc 0f 00 00  19 00 00 00  d7 0f 00 00  05 00 00 00   ................
    0030:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    0fd0:   00 00 00 00  00 00 00 53  6d 61 6c 6c  56 65 72 79   .......SmallVery
    0fe0:   20 76 65 72  79 20 76 65  72 79 20 6c  6f 6e 67 20    very very long 
    0ff0:   74 75 70 6c  65 48 65 6c  6c 6f 20 57  6f 72 6c 64   tupleHello World
    "###);

    assert_eq!(page.get_tuple(slot1).unwrap(), b"Hello World");
    assert_eq!(page.get_tuple(slot2).unwrap(), b"Very very very long tuple");
    assert_eq!(page.get_tuple(slot3).unwrap(), b"Small");

    Ok(())
}

#[test]
fn test_page_full() -> table_page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut page_data);
    page.insert_tuple(&[1u8; PAGE_SIZE - 100])?;
    assert_eq!(page.insert_tuple(&[1u8; 500]), Err(PageFull));

    Ok(())
}
