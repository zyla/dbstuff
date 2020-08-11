use std::sync::{Arc, Mutex};

use crate::net::*;

#[derive(Default)]
pub struct Net<M, S> {
    pub(crate) servers: Vec<S>,
    pub(crate) pending_messages: Vec<Envelope<M>>,
}

impl<M, S: Receiver<M>> Net<M, S> {
    pub fn deliver(&mut self) {
        for envelope in self.pending_messages.drain(0..) {
            self.servers[envelope.to].receive(envelope);
        }
    }
}

pub fn new_endpoint<M, S>(net: &Arc<Mutex<Net<M, S>>>, me: ServerId) -> Handle<M> {
    let net_ = net.clone();
    Handle(Arc::new(|to, msg| {
        net_.lock().unwrap().pending_messages.push(Envelope {
            from: me,
            to,
            msg
        });
    }))
}

#[derive(Clone)]
pub struct Handle<M>(Arc<dyn Fn(ServerId, M)>);

impl<M: Clone> Endpoint<M> for Handle<M> {
    fn send(&self, to: ServerId, msg: &M) {
        self.0(to, msg.clone());
    }
}
