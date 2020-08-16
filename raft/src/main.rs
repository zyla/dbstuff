//! A cluster that implements Single Decree Paxos.

#[macro_use]
extern crate serde_derive;

#[macro_use]
extern crate clap;

use serde_derive::{Deserialize, Serialize};
use stateright::actor::register::{
    RegisterMsg, RegisterMsg::*, RegisterTestSystem, TestRequestId, TestValue,
};
use stateright::actor::system::{model_peers, SystemModel, SystemState};
use stateright::actor::{majority, Actor, Id, Out};
use stateright::util::{HashableHashMap, HashableHashSet};
use stateright::Model;
use stateright::Property;
use std::time::Duration;

type Term = u32;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
enum Msg {
    RequestVote { term: Term },
    Vote { term: Term, granted: bool },
    AppendEntries { term: Term },
    AppendEntriesReply { term: Term },
}
use Msg::*;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    current_term: Term,
    voted_for: Option<Id>,
    role: Role,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Role {
    Leader,
    Candidate { votes_from: HashableHashSet<Id> },
    Follower,
}

use Role::*;

#[derive(Clone)]
struct Server {
    me: Id,
    servers: Vec<Id>,
}

impl Server {
    fn peers(&self) -> Vec<Id> {
        self.servers
            .iter()
            .copied()
            .filter(|x| *x != self.me)
            .collect()
    }
}

impl Actor for Server {
    type Msg = Msg;
    type State = State;

    fn on_start(&self, _id: Id, o: &mut Out<Self>) {
        o.set_state(State {
            current_term: 0,
            voted_for: None,
            role: Follower,
        });
        o.set_timer(Duration::from_millis(0)..Duration::from_millis(300));
    }

    fn on_msg(&self, _id: Id, state: &Self::State, src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        match msg {
            RequestVote { term } => {
                let candidate = src;
                if term < state.current_term {
                    return;
                }
                let mut state = state.clone();

                if term > state.current_term {
                    state.current_term = term;
                    state.voted_for = None;
                    state.role = Follower;
                }

                if let Some(vote) = state.voted_for {
                    o.send(
                        src,
                        Vote {
                            term,
                            granted: vote == candidate,
                        },
                    );
                } else {
                    state.voted_for = Some(candidate);
                    o.send(
                        src,
                        Vote {
                            term,
                            granted: true,
                        },
                    );
                }
                o.set_state(state);
            }
            Vote { term, granted } => {
                if term < state.current_term {
                    return;
                }
                let mut state = state.clone();

                if term > state.current_term {
                    state.current_term = term;
                    state.voted_for = None;
                    state.role = Follower;
                    o.set_state(state);
                    return;
                }

                if !granted {
                    return;
                }

                match state.role {
                    Candidate { mut votes_from } => {
                        votes_from.insert(src);
                        if votes_from.len() >= majority(self.servers.len()) {
                            state.voted_for = None;
                            state.role = Leader;
                            o.broadcast(
                                &self.peers(),
                                &AppendEntries {
                                    term: state.current_term,
                                },
                            );
                            o.set_timer(Duration::from_millis(30)..Duration::from_millis(30));
                            o.set_state(state);
                        }
                    }
                    _ => {
                        return;
                    }
                }
            }
            AppendEntries { term } => {
                if term < state.current_term {
                    return;
                }
                let mut state = state.clone();

                if term > state.current_term {
                    state.current_term = term;
                    state.voted_for = None;
                    state.role = Follower;
                    o.set_state(state);
                }
                o.send(src, AppendEntriesReply { term });
            }
            AppendEntriesReply { term } => {
                if term < state.current_term {
                    return;
                }
                let mut state = state.clone();

                if term > state.current_term {
                    state.current_term = term;
                    state.voted_for = None;
                    state.role = Follower;
                    o.set_state(state);
                }
            }
        }
    }

    fn on_timeout(&self, _id: Id, state: &Self::State, o: &mut Out<Self>) {
        match state.role {
            Leader => {
                o.broadcast(
                    &self.peers(),
                    &AppendEntries {
                        term: state.current_term,
                    },
                );
                o.set_timer(Duration::from_millis(30)..Duration::from_millis(30));
            }
            _ => {
                let mut state = state.clone();
                state.current_term += 1;
                state.voted_for = Some(self.me);
                let mut votes_from = HashableHashSet::new();
                votes_from.insert(self.me);
                state.role = Candidate { votes_from };
                let term = state.current_term;
                o.set_state(state);
                o.broadcast(&self.peers(), &RequestVote { term });
                o.set_timer(Duration::from_millis(150)..Duration::from_millis(300));
            }
        }
    }
}

#[derive(Clone)]
struct System {
    servers: Vec<Server>,
}

impl stateright::actor::system::System for System {
    type Actor = Server;
    type History = ();

    fn actors(&self) -> Vec<Self::Actor> {
        self.servers.clone()
    }

    fn properties(&self) -> Vec<Property<SystemModel<Self>>> {
        vec![]
    }
}

fn main() {
    use clap::{value_t, App, AppSettings, Arg, SubCommand};
    use stateright::actor::spawn::spawn;
    use stateright::actor::system::System;
    use stateright::explorer::Explorer;
    use std::net::{Ipv4Addr, SocketAddrV4};

    env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));

    let mut app = App::new("paxos")
        .about("single decree paxos")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(
            SubCommand::with_name("check")
                .about("model check")
                .arg(
                    Arg::with_name("put_count")
                        .help("number of puts")
                        .default_value("2"),
                )
                .arg(
                    Arg::with_name("get_count")
                        .help("number of gets")
                        .default_value("2"),
                ),
        )
        .subcommand(
            SubCommand::with_name("explore")
                .about("interactively explore state space")
                .arg(
                    Arg::with_name("put_count")
                        .help("number of puts")
                        .default_value("2"),
                )
                .arg(
                    Arg::with_name("get_count")
                        .help("number of gets")
                        .default_value("2"),
                )
                .arg(
                    Arg::with_name("address")
                        .help("address Explorer service should listen upon")
                        .default_value("localhost:3000"),
                ),
        )
        .subcommand(SubCommand::with_name("spawn").about("spawn with messaging over UDP"));
    let args = app.clone().get_matches();

    match args.subcommand() {
        ("explore", Some(args)) => {
            let servers = vec![Id::from(0), Id::from(1), Id::from(2)];
            let address = value_t!(args, "address", String).expect("address");
            crate::System {
                servers: vec![
                    Server {
                        me: Id::from(0),
                        servers: servers.clone(),
                    },
                    Server {
                        me: Id::from(1),
                        servers: servers.clone(),
                    },
                    Server {
                        me: Id::from(2),
                        servers: servers.clone(),
                    },
                ],
            }
            .into_model()
            .checker()
            .serve(address)
            .unwrap();
        }
        _ => app.print_help().unwrap(),
    }
}
