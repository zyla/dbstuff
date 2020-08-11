use std::sync::{Arc, Mutex};

use crate::net;
use crate::net::*;
use crate::server;
use crate::server::*;
use crate::simulated_net;
use crate::simulated_net::*;

fn make_net(n_servers: usize, cfg: Configuration) -> (Net<Message, Server>, Vec<Arc<Server>>) {
    let mut net = Net::new();
    let servers: Vec<Arc<Server>> = (0..n_servers)
        .map(|i| Arc::new(Server::new(i, net.new_endpoint(), cfg.clone())))
        .collect();
    net.servers.extend_from_slice(&servers);
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
    env_logger::init();

    let n_servers = 3;
    let txid = 1;
    let cfg = only_txreg(n_servers);
    let (net, servers) = make_net(n_servers + 1, cfg.clone());
    let client = &servers[3];
    let rx = client.rpc.send_to_all_other(
        &cfg.txreg_servers,
        Request::CreateTransaction { txid, leader_id: 0 },
    );
    std::thread::spawn(move || {
        net.deliver();
    });
    let mut num_success = 0;
    while let Ok((_from, reply)) = rx.recv() {
        match reply {
            Reply::Success => {
                num_success += 1;
                if num_success == 2 {
                    break;
                }
            }
            _ => {}
        }
    }
    debug!("{:?}", servers[0].transactions);
    debug!("{:?}", servers[1].transactions);
    debug!("{:?}", servers[2].transactions);
    let rx = client.rpc.send_request(
        1,
        Request::RequestFinalize {
            txid,
            status: TransactionStatus::Committed,
        },
    );
    let rx2 = client.rpc.send_request(
        2,
        Request::RequestFinalize {
            txid,
            status: TransactionStatus::Aborted,
        },
    );
    let rx3 = client.rpc.send_request(
        0,
        Request::RequestFinalize {
            txid,
            status: TransactionStatus::Committed,
        },
    );
    debug!("reply from follower 1: {:?}", rx.recv().unwrap());
    debug!("reply from follower 2: {:?}", rx2.recv().unwrap());
    debug!("reply from leader: {:?}", rx3.recv().unwrap());
    debug!("{:?}", servers[0].transactions);
    debug!("{:?}", servers[1].transactions);
    debug!("{:?}", servers[2].transactions);
}
