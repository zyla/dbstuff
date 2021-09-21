#[cfg(test)]
#[macro_use]
extern crate insta;

pub mod btree;
mod page;

#[cfg(test)]
mod btree_tests;
#[cfg(test)]
mod hexdump;
#[cfg(test)]
mod page_tests;
