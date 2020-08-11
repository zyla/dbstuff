use std::sync::{Arc, Mutex};

use crate::net::*;

pub struct Net<M, S> {
    servers: Vec<S>,
    pending_messages: Vec<Envelope<M>>,
}

impl<M, S: Receiver<M>> Net<M, S> {
    pub fn deliver(&mut self) {
        for envelope in self.pending_messages.drain(0..) {
            self.servers[envelope.to].receive(envelope);
        }
    }
}

pub fn new_endpoint<M, S>(net: &Arc<Mutex<Net<M, S>>>, me: ServerId) -> Handle<M, S> {
    Handle {
        me,
        net: net.clone(),
    }
}

pub struct Handle<M, S> {
    me: ServerId,
    net: Arc<Mutex<Net<M, S>>>,
}

impl<M, S> Clone for Handle<M, S> {
    fn clone(&self) -> Self {
        Handle {
            me: self.me,
            net: self.net.clone(),
        }
    }
}

impl<M: Clone, S> Endpoint<M> for Handle<M, S> {
    fn send(&self, to: ServerId, msg: &M) {
        self.net.lock().unwrap().pending_messages.push(Envelope {
            from: self.me,
            to,
            msg: msg.clone(),
        });
    }
}
