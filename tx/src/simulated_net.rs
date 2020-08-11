use std::sync::{mpsc, Arc, Mutex};

use crate::{net, net::*};

pub struct Net<M, S> {
    pub(crate) servers: Vec<S>,
    pub(crate) pending_messages: Vec<Envelope<M>>,
    pub(crate) rx: mpsc::Receiver<Envelope<M>>,
    pub(crate) tx: mpsc::Sender<Envelope<M>>,
}

impl<M, S> Net<M, S> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Net {
            servers: vec![],
            pending_messages: vec![],
            rx,
            tx,
        }
    }

    pub fn new_endpoint(&self) -> net::Endpoint<M> {
        self.tx.clone()
    }
}

impl<M, S: Receiver<M>> Net<M, S> {
    pub fn deliver(&mut self) {
        for envelope in self.pending_messages.drain(0..) {
            self.servers[envelope.to].receive(envelope);
        }
    }
}
