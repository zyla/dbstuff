#[macro_use]
extern crate serde_derive;

mod net;
mod server;

#[cfg(test)]
mod simulated_net;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
