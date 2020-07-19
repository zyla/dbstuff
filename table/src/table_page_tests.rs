use crate::hexdump::pretty_hex;
use crate::table_page;
use crate::table_page::TablePage;
use buffer_pool::disk_manager::{PageData, PAGE_SIZE};

#[test]
fn test_new_page() {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut page_data);
    assert_snapshot!(pretty_hex(&&page_data[..]), @r###"
    0000:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0010:   00 10 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0020:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    "###);
}

#[test]
fn test_alloc_tuple() -> table_page::Result<()> {
    let mut page_data = [0u8; PAGE_SIZE];
    let mut page = TablePage::new(&mut page_data);
    page.insert_tuple(b"Hello World")?;
    page.insert_tuple(b"Very very very long tuple")?;
    page.insert_tuple(b"Small")?;
    assert_snapshot!(pretty_hex(&&page_data[..]), @r###"
    0000:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0010:   d7 0f 00 00  03 00 00 00  f5 0f 00 00  0b 00 00 00   ................
    0020:   dc 0f 00 00  19 00 00 00  d7 0f 00 00  05 00 00 00   ................
    0030:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    0fd0:   00 00 00 00  00 00 00 53  6d 61 6c 6c  56 65 72 79   .......SmallVery
    0fe0:   20 76 65 72  79 20 76 65  72 79 20 6c  6f 6e 67 20    very very long 
    0ff0:   74 75 70 6c  65 48 65 6c  6c 6f 20 57  6f 72 6c 64   tupleHello World
    "###);
    Ok(())
}
