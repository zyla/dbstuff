pub struct Tuple<'a> {
    pub data: &'a [u8],
}

impl<'a> Tuple<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Tuple { data }
    }
}
