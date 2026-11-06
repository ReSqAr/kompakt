//! The distribution thread.

use std::sync::Arc;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Select, Sender};

use super::config::OverflowPolicy;
use super::producer::State;

pub(super) struct Subscriber<K, T> {
    key: K,
    tx: Sender<T>,
    /// Elements numbered below this were published before it subscribed.
    since: u64,
}

pub(super) fn spawn<K, T, F>(
    data_rx: Receiver<(u64, T)>,
    register_rx: Receiver<(K, Sender<T>, u64)>,
    shutdown_rx: Receiver<()>,
    route: F,
    overflow: OverflowPolicy,
    state: Arc<State>,
    done: Sender<()>,
) -> JoinHandle<()>
where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    thread::Builder::new()
        .name("fanout".to_owned())
        .spawn(move || {
            run(data_rx, register_rx, shutdown_rx, route, overflow, state);
            drop(done);
        })
        .expect("spawn fan-out thread")
}

pub(super) fn run<K, T, F>(
    data_rx: Receiver<(u64, T)>,
    register_rx: Receiver<(K, Sender<T>, u64)>,
    shutdown_rx: Receiver<()>,
    route: F,
    overflow: OverflowPolicy,
    state: Arc<State>,
) where
    K: Eq + Copy + Send + 'static,
    T: Clone + Send + 'static,
    F: Fn(&T) -> Option<K> + Send + 'static,
{
    let mut subscribers: Vec<Subscriber<K, T>> = Vec::new();
    let mut register_open = true;

    'fanout: loop {
        // Phase 1: one element, a registration, or shutdown.
        let mut sel = Select::new_biased();
        let shutdown_op = sel.recv(&shutdown_rx);
        let register_op = register_open.then(|| sel.recv(&register_rx));
        let data_op = sel.recv(&data_rx);
        let op = sel.select();

        if op.index() == shutdown_op {
            let _ = op.recv(&shutdown_rx);
            break 'fanout;
        }
        if Some(op.index()) == register_op {
            match op.recv(&register_rx) {
                Ok((key, tx, since)) => subscribers.push(Subscriber { key, tx, since }),
                Err(_) => register_open = false,
            }
            continue;
        }
        debug_assert_eq!(op.index(), data_op);
        let (seq, element) = match op.recv(&data_rx) {
            Ok(numbered) => numbered,
            Err(_) => break 'fanout,
        };

        // Phase 2: the current element's recipient set is fixed.
        let key = route(&element);
        let matches =
            |subscriber: &Subscriber<K, T>| key == Some(subscriber.key) && seq >= subscriber.since;

        match overflow {
            OverflowPolicy::Shed => {
                let mut dead = Vec::new();
                for (i, subscriber) in subscribers.iter().enumerate() {
                    if !matches(subscriber) {
                        continue;
                    }
                    if subscriber.tx.try_send(element.clone()).is_err() {
                        dead.push(i);
                    }
                }
                remove_descending(&mut subscribers, dead);
            }
            OverflowPolicy::Block => {
                let mut remaining: Vec<(usize, Sender<T>)> = subscribers
                    .iter()
                    .enumerate()
                    .filter(|(_, subscriber)| matches(subscriber))
                    .map(|(i, subscriber)| (i, subscriber.tx.clone()))
                    .collect();
                let mut dead = Vec::new();

                while !remaining.is_empty() {
                    let senders: Vec<Sender<T>> =
                        remaining.iter().map(|(_, tx)| tx.clone()).collect();
                    let mut sel = Select::new_biased();
                    let shutdown_op = sel.recv(&shutdown_rx);
                    let register_op = register_open.then(|| sel.recv(&register_rx));
                    let send_ops: Vec<usize> = senders.iter().map(|tx| sel.send(tx)).collect();
                    let op = sel.select();

                    if op.index() == shutdown_op {
                        let _ = op.recv(&shutdown_rx);
                        break 'fanout;
                    }
                    if Some(op.index()) == register_op {
                        match op.recv(&register_rx) {
                            Ok((key, tx, since)) => subscribers.push(Subscriber { key, tx, since }),
                            Err(_) => register_open = false,
                        }
                        continue;
                    }

                    let j = send_ops
                        .iter()
                        .position(|&i| i == op.index())
                        .expect("selected a send op");
                    let sent = op.send(&senders[j], element.clone());
                    let (subscriber_index, _) = remaining.swap_remove(j);
                    if sent.is_err() {
                        dead.push(subscriber_index);
                    }
                }
                remove_descending(&mut subscribers, dead);
            }
        }
    }

    // Close registration first, then drain queued registrations so their
    // subscriber senders drop even if the registry handle is still alive.
    let mut closed = state.closed.lock();
    *closed = true;
    while register_rx.try_recv().is_ok() {}
    drop(closed);
}

pub(super) fn remove_descending<K, T>(
    subscribers: &mut Vec<Subscriber<K, T>>,
    mut dead: Vec<usize>,
) {
    dead.sort_unstable();
    for index in dead.into_iter().rev() {
        subscribers.remove(index);
    }
}
