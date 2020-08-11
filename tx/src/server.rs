use std::collections::hash_map;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::net;
use net::*;

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

pub(crate) struct Rpc {
    me: ServerId,
    endpoint: Mutex<net::Endpoint<Message>>,
    next_request_id: AtomicUsize,
    reply_waiters: Mutex<HashMap<usize, mpsc::SyncSender<(ServerId, Reply)>>>,
}

impl Rpc {
    pub(crate) fn new(me: ServerId, endpoint: net::Endpoint<Message>) -> Self {
        Rpc {
            me,
            endpoint: Mutex::new(endpoint),
            next_request_id: AtomicUsize::new(0),
            reply_waiters: Default::default(),
        }
    }

    fn receive(
        &self,
        msg: Envelope<Message>,
        process_request: impl FnOnce(ServerId, Request) -> Reply,
    ) {
        match msg.msg {
            Message::Request(id, request) => {
                let reply = process_request(msg.from, request);
                self.endpoint
                    .lock()
                    .unwrap()
                    .send(Envelope {
                        from: self.me,
                        to: msg.from,
                        msg: Message::Reply(id, reply),
                    })
                    .unwrap();
            }
            Message::Reply(id, reply) => {
                if let Some(waiter) = self.reply_waiters.lock().unwrap().remove(&id) {
                    let _ = waiter.send((msg.from, reply));
                }
            }
        }
    }

    pub(crate) fn send_request(
        &self,
        to: ServerId,
        request: Request,
    ) -> mpsc::Receiver<(ServerId, Reply)> {
        let (tx, rx) = mpsc::sync_channel(1);
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.reply_waiters.lock().unwrap().insert(id, tx.clone());
        self.endpoint
            .lock()
            .unwrap()
            .send(Envelope {
                from: self.me,
                to,
                msg: Message::Request(id, request.clone()),
            })
            .unwrap();
        rx
    }

    pub(crate) fn send_to_all_other(
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
                .lock()
                .unwrap()
                .send(Envelope {
                    from: self.me,
                    to: server,
                    msg: Message::Request(id, request.clone()),
                })
                .unwrap();
        }
        rx
    }
}

#[derive(Clone)]
pub struct Server(Arc<ServerInner>);

impl std::ops::Deref for Server {
    type Target = ServerInner;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl Server {
    pub fn new(me: ServerId, endpoint: net::Endpoint<Message>, cfg: Configuration) -> Self {
        Self(Arc::new(ServerInner {
            me,
            cfg,
            rpc: Rpc::new(me, endpoint),
            transactions: Default::default(),
            store: Default::default(),
        }))
    }
}

impl Receiver<Message> for Server {
    fn receive(&self, msg: Envelope<Message>) {
        self.rpc
            .receive(msg, |from, request| self.process_request(from, request));
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

#[derive(Debug)]
pub struct Transaction {
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

#[derive(Clone, Debug)]
pub struct Configuration {
    pub txreg_servers: Vec<ServerId>,
    pub range_servers: Vec<Vec<ServerId>>,
}

pub struct ServerInner {
    pub(crate) me: ServerId,
    cfg: Configuration,
    pub(crate) rpc: Rpc,
    pub(crate) transactions: Mutex<HashMap<TxId, Transaction>>,
    store: Mutex<HashMap<Key, HashMap<Version, Value>>>,
}

// process_request

impl Server {
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

                let mut previous: Option<TransactionStatus> = None;
                'retry: loop {
                    if tx.leader_id() == self.me {
                        tx.status = previous.unwrap_or(status);
                        let rx = self.rpc.send_to_all_other(
                            &self.cfg.txreg_servers,
                            Finalize {
                                txid,
                                ballot: tx.ballot,
                                status: tx.status,
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
                            .rpc
                            .send_to_all_other(&self.cfg.txreg_servers, Prepare { txid, ballot });

                        let mut num_successes = 0;
                        let num_successes_needed = self.cfg.txreg_servers.len() / 2; // majority minus me

                        while let Ok((_, reply)) = rx.recv() {
                            match reply {
                                Promise(previous_) => {
                                    previous = previous_.map(|x| x.1);
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
