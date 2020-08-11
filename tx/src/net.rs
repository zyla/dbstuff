use std::sync::mpsc;

pub type ServerId = usize;

pub struct Envelope<M> {
    pub from: ServerId,
    pub to: ServerId,
    pub msg: M,
}

pub type Endpoint<M> = mpsc::Sender<Envelope<M>>;

pub trait Receiver<M> {
    fn receive(&self, envelope: Envelope<M>);
}
