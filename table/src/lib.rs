#[cfg(test)]
#[macro_use]
extern crate insta;

pub mod table_heap;
pub mod table_page;

#[cfg(test)]
mod hexdump;
#[cfg(test)]
mod table_heap_tests;
#[cfg(test)]
mod table_page_tests;

pub mod datum;
#[cfg(test)]
pub mod datum_serialization_tests;
