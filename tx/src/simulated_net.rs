use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use crate::{net, net::*};

pub struct Net<M, S> {
    pub(crate) servers: Vec<Arc<S>>,
    pub(crate) rx: mpsc::Receiver<Envelope<M>>,
    pub(crate) tx: mpsc::Sender<Envelope<M>>,
}

impl<M, S> Net<M, S> {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Net {
            servers: vec![],
            rx,
            tx,
        }
    }

    pub fn new_endpoint(&self) -> net::Endpoint<M> {
        self.tx.clone()
    }
}

impl<M: std::fmt::Debug + Send + Sync + 'static, S: Receiver<M> + Send + Sync + 'static> Net<M, S> {
    pub fn deliver(&self) {
        while let Ok(envelope) = self.rx.recv_timeout(Duration::from_millis(100)) {
            debug!("{:?}", envelope);
            let server = self.servers[envelope.to].clone();
            std::thread::spawn(move || {
                server.receive(envelope);
            });
        }
    }
}
