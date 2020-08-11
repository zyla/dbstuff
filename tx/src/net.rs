pub type ServerId = usize;

pub struct Envelope<M> {
    pub from: ServerId,
    pub to: ServerId,
    pub msg: M,
}

pub trait Endpoint<M>: Clone {
    fn send(&self, to: ServerId, msg: M);
}

pub trait Receiver<M> {
    fn receive(&self, envelope: Envelope<M>);
}
