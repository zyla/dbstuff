#[cfg(test)]
#[macro_use]
extern crate insta;

pub mod btree;
mod internal_page;

#[cfg(test)]
mod hexdump;
#[cfg(test)]
mod page_tests;
