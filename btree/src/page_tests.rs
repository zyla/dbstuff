use crate::hexdump::pretty_hex;
use crate::internal_page::*;
use buffer_pool::disk_manager::PAGE_SIZE;

#[test]
fn test_new_internal_page() {
    let mut page_data = [0u8; PAGE_SIZE];
    InternalPage::new(&mut page_data);
    assert_snapshot!(pretty_hex(&&page_data[..]), @r###"
    0000:   00 00 00 10  00 00 00 00  00 00 00 00  00 00 00 00   ................
    0010:   00 00 00 00  00 00 00 00  00 00 00 00  00 00 00 00   ................
    *
    "###);
}
