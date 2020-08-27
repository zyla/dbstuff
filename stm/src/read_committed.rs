// Don't want to deal with adding `pub` everywhere for now
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

type TxId = usize;

#[derive(Debug, Clone)]
struct Tx {
    id: TxId,
    mu: Arc<Mutex<TxInner>>,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum TxState {
    InProgress,
    Committed,
    Aborted,
}

use TxState::*;

type Seen = HashSet<TxId>;

#[derive(Debug)]
struct TxInner {
    state: TxState,
    seen: Seen,
}

impl Tx {
    fn new_with_id(id: TxId) -> Self {
        assert!(id != 0);
        Tx {
            id,
            mu: Arc::new(Mutex::new(TxInner {
                state: InProgress,
                seen: Default::default(),
            })),
        }
    }

    fn id(&self) -> TxId {
        self.id
    }

    fn get_state(&self) -> (TxState, Seen) {
        let inner = self.mu.lock().unwrap();
        (inner.state, inner.seen.clone())
    }

    fn mark_txs_as_seen(&self, txs: &[TxId]) {
        let mut inner = self.mu.lock().unwrap();
        assert!(inner.state == InProgress);
        inner.seen.extend(txs);
    }

    fn commit(self) {
        let mut inner = self.mu.lock().unwrap();
        assert!(inner.state == InProgress);
        inner.state = Committed;
    }

    fn abort(self) {
        let mut inner = self.mu.lock().unwrap();
        assert!(inner.state == InProgress);
        inner.state = Aborted;
    }
}

#[derive(Debug, Clone)]
struct Var<T> {
    mu: Arc<Mutex<VarInner<T>>>,
}

#[derive(Debug, Clone)]
struct VarInner<T> {
    initial_value: T,
    versions: Vec<(Tx, T)>,
}

impl<T: Clone + std::fmt::Debug> Var<T> {
    fn new(initial_value: T) -> Self {
        Var {
            mu: Arc::new(Mutex::new(VarInner {
                initial_value,
                versions: vec![],
            })),
        }
    }

    fn read(&self, my_tx: &Tx) -> T {
        let inner = self.mu.lock().unwrap().clone();
        let mut my_value = None;
        let tx_states = inner.versions.iter().map(|(tx, value)| {
            if tx.id() == my_tx.id() {
                my_value = Some(value);
            }
            let (state, seen) = tx.get_state();
            (tx.id(), state, seen, value)
        }).filter(|(_, state, _, _)| *state == Committed).collect::<Vec<_>>();
        if let Some(v) = my_value {
            return v.clone();
        }
        if tx_states.is_empty() {
            return inner.initial_value;
        }
        let mut g = petgraph::graphmap::GraphMap::<TxId, (), petgraph::Directed>::new();
        for (txid, _, seen, _) in &tx_states {
            g.add_node(*txid);
            g.extend(seen.iter().map(|seen_txid| (*txid, *seen_txid)));
        }
        // FIXME: we need to get the full picture - we need to not only get `seen` from txs which
        // wrote to this variable, but also from txs which it has seen! (and recursively, until we
        // have no more to get).
        println!("{:?}", tx_states);
        let sccs = petgraph::algo::kosaraju_scc(&g);
        println!("{:?}", sccs);
        let last_scc = &sccs[sccs.len() - 1];
        let winning_txid = last_scc.iter().max().unwrap();
        tx_states.iter().find_map(|(txid, _, _, value)| if txid == winning_txid { Some((*value).clone()) } else { None }).unwrap()
    }

    fn write(&self, my_tx: &Tx, value: T) {
        let mut inner = self.mu.lock().unwrap();
        let txs_seen = inner.versions.iter().map(|(tx, _)| tx.id()).collect::<Vec<_>>();
        inner.versions.push((my_tx.clone(), value));
        drop(inner);

        my_tx.mark_txs_as_seen(&txs_seen);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_initial_value() {
        let tx1 = Tx::new_with_id(1);
        let x = Var::new(0);
        assert_eq!(x.read(&tx1), 0);
    }

    #[test]
    fn test_read_own_value() {
        let tx1 = Tx::new_with_id(1);
        let x = Var::new(0);
        x.write(&tx1, 1);
        assert_eq!(x.read(&tx1), 1);
    }

    #[test]
    fn test_read_initial_value_with_concurrent_tx() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        x.write(&tx2, 2);
        assert_eq!(x.read(&tx1), 0);
    }

    #[test]
    fn test_read_own_value_with_concurrent_tx() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        x.write(&tx1, 1);
        x.write(&tx2, 2);
        assert_eq!(x.read(&tx1), 1);
    }

    #[test]
    fn test_read_initial_value_with_aborted_tx() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        x.write(&tx2, 2);
        tx2.abort();
        assert_eq!(x.read(&tx1), 0);
    }

    #[test]
    fn test_read_value_from_committed_tx() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        x.write(&tx2, 2);
        tx2.commit();
        assert_eq!(x.read(&tx1), 2);
    }

    #[test]
    fn test_read_value_from_two_committed_txs() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);

        x.write(&tx2, 2);
        tx2.commit();

        x.write(&tx3, 3);
        tx3.commit();

        assert_eq!(x.read(&tx1), 3);
    }

    #[test]
    fn test_weird_ordering_1() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);

        x.write(&tx2, 2);
        x.write(&tx3, 3);
        tx2.commit();

        assert_eq!(x.read(&tx1), 2);
    }

    #[test]
    fn test_weird_ordering_2() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);

        x.write(&tx2, 2);
        x.write(&tx3, 3);
        tx3.commit();

        assert_eq!(x.read(&tx1), 3);

        tx2.commit();

        assert_eq!(x.read(&tx1), 3);
    }

    // Allowed by Read Committed, disallowed by Monotonic Atomic View
    // (see <https://jepsen.io/consistency/models/monotonic-atomic-view>)
    // (If I understand it correctly. After re-reading the article, it occured to me that it may be
    // the case that MAV still allows this outcome, but disallows the opposite one (2, 0) - hence
    // "monotonic")
    #[test]
    fn test_tearing() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        let y = Var::new(0);

        x.write(&tx2, 2);
        y.write(&tx2, 2);

        let x1 = x.read(&tx1);
        tx2.commit();
        let y1 = y.read(&tx1);

        assert_eq!((x1, y1), (0, 2));
    }

    #[test]
    fn test_dirty_write() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let x = Var::new(0);
        let y = Var::new(0);

        x.write(&tx1, 1);
        y.write(&tx2, 2);

        // Note: Postgres just takes a lock in this case, and the following two operations cause
        // the transactions to deadlock (and one of them is aborted by the deadlock detector).
        x.write(&tx2, 2);
        y.write(&tx1, 1);

        tx1.commit();
        tx2.commit();

        let tx3 = Tx::new_with_id(3);
        let x1 = x.read(&tx3);
        let y1 = y.read(&tx3);

        // According to <https://jepsen.io/consistency/models/read-uncommitted>:
        // > Read uncommitted is a consistency model which prohibits dirty writes,
        // > where two transactions modify the same object concurrently before committing.
        // So indeed we should disallow (1, 2) and (2, 1).
        //
        // Berenson et al. say[1], as I understand it, that we can't even allow concurrent transactions
        // to execute a write to the same object. But why should we care about what happens inside
        // a transaction until it commits?
        // "If T1 or T2 then performs a ROLLBACK, it is unclear what the correct data value should
        // be." - in our case it's clear - we just decide which transaction wins when they commit.
        //
        // So both (1, 1) and (2, 2) are legal outcomes. In our case (2, 2) is the result because
        // we choose the lexicographically highest txid in case of a ww-dependency cycle.
        //
        // [1]: "A Critique of ANSI SQL Isolation Levels" <https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf>
        assert_eq!((x1, y1), (2, 2));
    }

    #[test]
    fn test_dirty_write_2() {
        // In this case we add a third writer transaction which has a ww-dependency on both tx1 and
        // tx2. It should win.
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);
        let y = Var::new(0);

        x.write(&tx1, 1);
        y.write(&tx2, 2);

        x.write(&tx2, 2);
        y.write(&tx1, 1);

        x.write(&tx3, 3);
        y.write(&tx3, 3);

        tx1.commit();
        tx2.commit();
        tx3.commit();

        let tx4 = Tx::new_with_id(4);
        let x1 = x.read(&tx4);
        let y1 = y.read(&tx4);

        assert_eq!((x1, y1), (3, 3));
    }

    #[test]
    fn test_dirty_write_3() {
        // Inverse case than test_dirty_write_2 - tx3 is before both tx1 and tx2. The (tx1, tx2)
        // cycle wins, and tx2 is chosen because it's lexicographically highest.
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);
        let y = Var::new(0);

        x.write(&tx3, 3);
        y.write(&tx3, 3);

        x.write(&tx1, 1);
        y.write(&tx2, 2);

        x.write(&tx2, 2);
        y.write(&tx1, 1);

        tx1.commit();
        tx2.commit();
        tx3.commit();

        let tx4 = Tx::new_with_id(4);
        let x1 = x.read(&tx4);
        let y1 = y.read(&tx4);

        assert_eq!((x1, y1), (2, 2));
    }

    #[test]
    fn test_dirty_write_big_cycle() {
        let tx1 = Tx::new_with_id(1);
        let tx2 = Tx::new_with_id(2);
        let tx3 = Tx::new_with_id(3);
        let x = Var::new(0);
        let y = Var::new(0);
        let z = Var::new(0);

        // tx1 -> tx2
        x.write(&tx1, 1);
        x.write(&tx2, 2);

        // tx2 -> tx3
        y.write(&tx2, 2);
        y.write(&tx3, 3);

        // tx3 -> tx1
        z.write(&tx3, 3);
        z.write(&tx1, 1);

        tx1.commit();
        tx2.commit();
        tx3.commit();

        let tx4 = Tx::new_with_id(4);
        let x1 = x.read(&tx4);
        let y1 = y.read(&tx4);
        let z1 = z.read(&tx4);

        assert_eq!((x1, y1, z1), (3, 3, 3));
    }
}
