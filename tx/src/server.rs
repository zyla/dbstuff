use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use crate::net::*;

pub type TxId = usize;

#[derive(Serialize, Deserialize)]
pub enum Message {
    Request(usize, Request),
    Reply(usize, Reply),
}

#[derive(Serialize, Deserialize)]
pub enum Request {
    CreateTransaction { txid: TxId, leader_id: ServerId },
}

#[derive(Serialize, Deserialize)]
pub enum Reply {
    Ok,
}

#[derive(Clone)]
pub struct Server<E>(Arc<ServerInner<E>>);

impl<E> std::ops::Deref for Server<E> {
    type Target = ServerInner<E>;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

pub struct ServerInner<E> {
    me: ServerId,
    endpoint: E,
    next_request_id: AtomicUsize,
    reply_waiters: Mutex<HashMap<usize, mpsc::Receiver<Reply>>>,
}

impl<E: Endpoint<Message>> Server<E> {
    pub fn new(me: ServerId, endpoint: E) -> Self {
        Self(Arc::new(ServerInner {
            me,
            endpoint,
            next_request_id: AtomicUsize::new(0),
            reply_waiters: Default::default(),
        }))
    }

    fn process_request(&self, request: Request) -> Reply {
        Reply::Ok
    }
}

impl<E: Endpoint<Message>> Receiver<Message> for Server<E> {
    fn receive(&self, msg: Envelope<Message>) {
        match msg.msg {
            Message::Request(id, request) => {
                let reply = self.process_request(request);
                self.endpoint.send(msg.from, Message::Reply(id, reply));
            }
            Message::Reply(id, reply) => {}
        }
    }
}
