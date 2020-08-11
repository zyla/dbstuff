use std::sync::{Arc, Mutex};

use crate::net;
use crate::net::*;
use crate::server;
use crate::server::*;
use crate::simulated_net;
use crate::simulated_net::*;

fn make_net(
    n_servers: usize,
    cfg: Configuration,
) -> (Arc<Mutex<Net<Message, Server>>>, Vec<Server>) {
    let net = Arc::new(Mutex::new(Net::new()));
    let mut net_ = net.lock().unwrap();
    let servers: Vec<Server> = (0..n_servers)
        .map(|i| Server::new(i, net_.new_endpoint(), cfg.clone()))
        .collect();
    net_.servers.extend_from_slice(&servers);
    drop(net_);
    (net, servers)
}

fn only_txreg(n_servers: usize) -> Configuration {
    Configuration {
        txreg_servers: (0..n_servers).collect(),
        range_servers: vec![],
    }
}

#[test]
fn basic_test() {
    let n_servers = 3;
    let (net, servers) = make_net(n_servers, only_txreg(n_servers));
}
