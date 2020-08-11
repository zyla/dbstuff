use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::net::*;

pub type TxId = usize;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type Version = usize;

pub type Ballot = (usize, ServerId);

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Request(usize, Request),
    Reply(usize, Reply),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Request {
    CreateTransaction {
        txid: TxId,
        leader_id: ServerId,
    },
    RequestFinalize {
        txid: TxId,
        status: TransactionStatus,
    },
    Prepare {
        txid: TxId,
        ballot: Ballot,
    },
    Finalize {
        txid: TxId,
        ballot: Ballot,
        status: TransactionStatus,
    },
    MarkFinalized {
        txid: TxId,
        status: TransactionStatus,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Reply {
    // Generic reply
    Success,

    // Generic reply to transaction-related messages
    UnknownTransaction,

    // Reply to Finalize and RequestFinalize
    AlreadyFinalized(TransactionStatus),

    // Reply to Finalize and RequestFinalize
    TransactionLeaderChanged { ballot: Ballot },

    // Reply to Prepare
    // Argument is the last proposed status
    Promise(Option<(Ballot, TransactionStatus)>),

    UnknownError,
}

#[derive(Clone)]
pub struct Server<E>(Arc<ServerInner<E>>);

impl<E> std::ops::Deref for Server<E> {
    type Target = ServerInner<E>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl<E: Endpoint<Message>> Server<E> {
    pub fn new(me: ServerId, endpoint: E, cfg: Configuration) -> Self {
        Self(Arc::new(ServerInner {
            me,
            endpoint,
            cfg,
            next_request_id: AtomicUsize::new(0),
            reply_waiters: Default::default(),
            transactions: Default::default(),
            store: Default::default(),
        }))
    }

    fn send_request(&self, to: ServerId, request: Request) -> Reply {
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::sync_channel(1);
        self.reply_waiters.lock().unwrap().insert(id, tx);
        self.endpoint.send(to, &Message::Request(id, request));
        rx.recv().unwrap().1
    }

    fn send_to_all_other(
        &self,
        servers: &[ServerId],
        request: Request,
    ) -> mpsc::Receiver<(ServerId, Reply)> {
        let (tx, rx) = mpsc::sync_channel(servers.len());
        for &server in servers {
            if server == self.me {
                continue;
            }
            let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
            self.reply_waiters.lock().unwrap().insert(id, tx.clone());
            self.endpoint
                .send(server, &Message::Request(id, request.clone()));
        }
        rx
    }
}

impl<E: Endpoint<Message>> Receiver<Message> for Server<E> {
    fn receive(&self, msg: Envelope<Message>) {
        match msg.msg {
            Message::Request(id, request) => {
                let reply = self.process_request(msg.from, request);
                self.endpoint.send(msg.from, &Message::Reply(id, reply));
            }
            Message::Reply(id, reply) => {
                if let Some(waiter) = self.reply_waiters.lock().unwrap().remove(&id) {
                    waiter.send((msg.from, reply)).unwrap();
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Copy, Clone)]
pub enum TransactionStatus {
    InProgress,
    Committed,
    Aborted,
}

impl TransactionStatus {
    pub fn determined(self) -> bool {
        self == Self::InProgress
    }
}

struct Transaction {
    ballot: Ballot,
    status: TransactionStatus,
    finalized: bool,
}

impl Transaction {
    fn new(leader_id: ServerId) -> Self {
        Self {
            ballot: (1, leader_id),
            status: TransactionStatus::InProgress,
            finalized: false,
        }
    }

    fn leader_id(&self) -> ServerId {
        self.ballot.1
    }
}

pub struct Configuration {
    txreg_servers: Vec<ServerId>,
    range_servers: Vec<Vec<ServerId>>,
}

pub struct ServerInner<E> {
    me: ServerId,
    cfg: Configuration,
    endpoint: E,
    next_request_id: AtomicUsize,
    reply_waiters: Mutex<HashMap<usize, mpsc::SyncSender<(ServerId, Reply)>>>,
    transactions: Mutex<HashMap<TxId, Transaction>>,
    store: Mutex<HashMap<Key, HashMap<Version, Value>>>,
}

// process_request

impl<E: Endpoint<Message>> Server<E> {
    fn process_request(&self, _from: ServerId, request: Request) -> Reply {
        use Reply::*;
        use Request::*;
        match request {
            CreateTransaction { txid, leader_id } => {
                self.transactions
                    .lock()
                    .unwrap()
                    .entry(txid)
                    .or_insert_with(|| Transaction::new(leader_id));
                Success
            }
            RequestFinalize { txid, status } => {
                let mut transactions = self.transactions.lock().unwrap();
                let tx = match transactions.get_mut(&txid) {
                    Some(tx) => tx,
                    None => return UnknownTransaction,
                };

                if tx.finalized {
                    if tx.status == status {
                        return Success;
                    } else {
                        return AlreadyFinalized(tx.status);
                    }
                }

                'retry: loop {
                    if tx.leader_id() == self.me {
                        tx.status = status;
                        let rx = self.send_to_all_other(
                            &self.cfg.txreg_servers,
                            Finalize {
                                txid,
                                ballot: tx.ballot,
                                status,
                            },
                        );

                        let mut num_successes = 0;
                        let num_successes_needed = self.cfg.txreg_servers.len() / 2; // majority minus me

                        while let Ok((_, reply)) = rx.recv() {
                            match reply {
                                Success => {
                                    num_successes += 1;
                                    if num_successes >= num_successes_needed {
                                        tx.status = status;
                                        tx.finalized = true;
                                        return Success;
                                    }
                                }
                                AlreadyFinalized(status) => {
                                    tx.status = status;
                                    tx.finalized = true;
                                    return reply;
                                }
                                TransactionLeaderChanged { ballot } => {
                                    tx.ballot = ballot;
                                    return reply;
                                }
                                _ => {
                                    error!("unknown reply to Finalize: {:?}", reply);
                                }
                            }
                        }
                        return UnknownError;
                    } else {
                        let ballot = (tx.ballot.0 + 1, self.me);
                        let rx = self
                            .send_to_all_other(&self.cfg.txreg_servers, Prepare { txid, ballot });

                        let mut num_successes = 0;
                        let num_successes_needed = self.cfg.txreg_servers.len() / 2; // majority minus me

                        while let Ok((_, reply)) = rx.recv() {
                            match reply {
                                Success => {
                                    num_successes += 1;
                                    if num_successes >= num_successes_needed {
                                        tx.ballot = ballot;
                                        continue 'retry;
                                    }
                                }
                                AlreadyFinalized(status) => {
                                    tx.status = status;
                                    tx.finalized = true;
                                    return reply;
                                }
                                TransactionLeaderChanged { ballot } => {
                                    tx.ballot = ballot;
                                    return reply;
                                }
                                _ => {
                                    error!("unknown reply to Prepare: {:?}", reply);
                                }
                            }
                        }
                        return UnknownError;
                    }
                }
            }
            Prepare { txid, ballot } => {
                let mut transactions = self.transactions.lock().unwrap();
                let tx = match transactions.get_mut(&txid) {
                    Some(tx) => tx,
                    None => return UnknownTransaction,
                };

                if tx.finalized {
                    return AlreadyFinalized(tx.status);
                }

                if tx.ballot > ballot {
                    return TransactionLeaderChanged { ballot: tx.ballot };
                }

                let previous = if tx.status.determined() {
                    Some((tx.ballot, tx.status))
                } else {
                    None
                };

                tx.ballot = ballot;
                Promise(previous)
            }
            Finalize {
                txid,
                ballot,
                status,
            } => {
                let mut transactions = self.transactions.lock().unwrap();
                let tx = match transactions.get_mut(&txid) {
                    Some(tx) => tx,
                    None => return UnknownTransaction,
                };

                if tx.finalized {
                    if tx.status == status {
                        return Success;
                    } else {
                        return AlreadyFinalized(tx.status);
                    }
                }

                if tx.ballot > ballot {
                    return TransactionLeaderChanged { ballot: tx.ballot };
                }

                tx.status = status;
                Success
            }
            MarkFinalized { txid, status } => {
                let mut transactions = self.transactions.lock().unwrap();
                let tx = match transactions.get_mut(&txid) {
                    Some(tx) => tx,
                    None => return UnknownTransaction,
                };
                tx.status = status;
                tx.finalized = true;
                return Success;
            }
        }
    }
}
