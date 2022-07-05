use omnipaxos_core::ballot_leader_election::messages::BLEMessage;
use omnipaxos_core::messages::{Message, PaxosMsg};

use omnipaxos_core::util::LogEntry::Decided;
use omnipaxos_core::{
    ballot_leader_election::{BLEConfig, BallotLeaderElection},
    sequence_paxos::{CompactionErr, ReconfigurationRequest, SequencePaxos, SequencePaxosConfig},
    storage::{memory_storage::MemoryStorage, Entry, Snapshot, Storage},
    util::LogEntry,
};

use std::str;
use std::{env, vec};
use std::{thread, time};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

pub struct SequencePaxosHandle<T: Entry, S: Snapshot<T>> {
    pub incoming: mpsc::Sender<Message<T, S>>,
    pub outgoing: mpsc::Receiver<Message<T, S>>,
}

#[tokio::main]
async fn main() {
    //for this project example we create three nodes in a cluster, easy to refactor into n nodes though
    let args: Vec<String> = env::args().collect();

    let my_pid_string = &args[1];
    let my_pid = my_pid_string.parse::<u64>().unwrap();
    let peer1_string = &args[2];
    let peer2_string = &args[3];
    let peer1 = peer1_string.parse::<u64>().unwrap();
    let peer2 = peer2_string.parse::<u64>().unwrap();

    let my_peers = vec![peer1, peer2];
    let seq_port = 50000 + (my_pid as u16);
    let ble_port = 60000 + (my_pid as u16);
    let seq_listener_adr = format!("127.0.0.1:{}", seq_port);
    let ble_listener_adr = format!("127.0.0.1:{}", ble_port);

    let configuration_id = 1;
    let mut sp_config = SequencePaxosConfig::default();
    sp_config.set_configuration_id(configuration_id);
    sp_config.set_pid(my_pid);
    sp_config.set_peers(my_peers.clone());

    let storage = MemoryStorage::<KeyValue, ()>::default();
    let mut seq_paxos = SequencePaxos::with(sp_config, storage);

    let SEQ_listener = TcpListener::bind(seq_listener_adr).await.unwrap();
    let BLE_listener = TcpListener::bind(ble_listener_adr).await.unwrap();

    let mut ble_config = BLEConfig::default();
    ble_config.set_pid(my_pid);
    ble_config.set_peers(my_peers);
    ble_config.set_hb_delay(40);

    let mut ble = BallotLeaderElection::with(ble_config);

    let mut stdin = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    //our channels
    let (handleSender, mut handleReceiver): (
        tokio::sync::mpsc::Sender<(String, Vec<u8>)>,
        tokio::sync::mpsc::Receiver<(String, Vec<u8>)>,
    ) = mpsc::channel(32);
    let receiveSEQFromNetwork = handleSender.clone();
    let sendSEQmessages = handleSender.clone();
    let stdinConsole = handleSender.clone();
    let sendPeriodically = handleSender.clone();
    let sendTickrate = handleSender.clone();
    let sendHeartbeat = handleSender.clone();

    //receive messages from network layer and send to handle
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = SEQ_listener.accept().await.unwrap();
            println!("accepted connection");
            let mut buffer = vec![1; 256];

            loop {
                let n = socket.read(&mut buffer).await.unwrap();

                if n == 0 {
                    break;
                }
                receiveSEQFromNetwork
                    .send((String::from("handle"), (&buffer[..n]).to_vec()))
                    .await
                    .unwrap();
            }
        }
    });

    //send messages out to everyone
    tokio::spawn(async move {
        loop {
            let msg = String::from("updateEveryone");
            thread::sleep(time::Duration::from_millis(20));
            let placeholder = vec![];
            sendPeriodically.send((msg, placeholder)).await.unwrap();
        }
    });

    //selects that waits for incoming messages written in console
    tokio::spawn(async move {
        loop {
            tokio::select! {
                line = stdin.next_line() => {
                    match line {
                        Ok(Some(string)) => {
                            println!("You wrote: {:?}", string);
                            let placeholder = vec![];

                            stdinConsole.send((string, placeholder)).await.unwrap();
                        }
                        Err(e) => println!("error when trying to establish new connection"),
                        Ok(None) => println!("no line read when trying to establish new connection"),
                    }
                }
            }
        }
    });

    //sets tickrate for BLE.
    tokio::spawn(async move {
        loop {
            let msg = String::from("election");
            thread::sleep(time::Duration::from_millis(100));

            let placeholder = vec![];
            handleSender.send((msg, placeholder)).await.unwrap();
        }
    });

    //sets heartbeats
    tokio::spawn(async move {
        loop {
            let (mut socket, _) = BLE_listener.accept().await.unwrap();
            println!("BLE heartbeat connection");
            let mut buffer = vec![1; 256];
            loop {
                let n = socket.read(&mut buffer).await.unwrap();

                if n == 0 {
                    break;
                }

                // tuple with heartbeat string and heartbeat msg
                sendHeartbeat
                    .send((String::from("heartbeat"), (&buffer[..n]).to_vec()))
                    .await
                    .unwrap();
            }
        }
    });

    //threadhandler that owns all the instantiated structs.
    tokio::spawn(async move {
        while let Some(msg) = handleReceiver.recv().await {
            let (string, message) = msg;
            match string.as_str() {
                "heartbeat" => {
                    println!("Sending heartbeat");
                    let msg: BLEMessage = bincode::deserialize(&message).unwrap();
                    ble.handle(msg);
                }

                "election" => {
                    if let Some(leader) = ble.tick() {
                        seq_paxos.handle_leader(leader);
                    }
                }

                "handle" => {
                    let decoded: Message<KeyValue, ()> = bincode::deserialize(&message).unwrap();
                    println!("Handling seqpaq struct");
                    seq_paxos.handle(decoded);
                }

                "updateEveryone" => {
                    for msg in seq_paxos.get_outgoing_msgs() {
                        let peer_port = 50000 + (msg.to as u16);
                        let peer_address = format!("127.0.0.1:{}", peer_port);
                        // let mut stream = TcpStream::connect(peer_address).await.unwrap();
                        let mut streamMatch = None;
                        match TcpStream::connect(peer_address).await{
                            Ok(result) => streamMatch = Some(result),
                            Err(e) => println!("connection refused, re-trying"),

                        }
                        // let mut streamMatch =  match TcpStream::connect(peer_address).await.unwrap();
                        if let Some(mut stream) = streamMatch{
                            let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();
                            stream.write_all(&encoded).await.unwrap();
                            println!("sent BLE heartbeat to {}", &msg.to);
                            
                        }
                        // let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();
                        // stream.write_all(&encoded).await.unwrap();
                        // println!("updatEveryone called, sending to {}", &msg.to);
                    }
                    for msg in ble.get_outgoing_msgs() {
                        let peer_port = 60000 + (msg.to as u16);
                        let peer_address = format!("127.0.0.1:{}", peer_port);
                        // let mut stream = TcpStream::connect(peer_address).await.unwrap();

                        let mut streamMatch = None;
                        match TcpStream::connect(peer_address).await{
                            Ok(result) => streamMatch = Some(result),
                            Err(e) => println!("connection refused, re-trying"),

                        }
                        // let mut streamMatch =  match TcpStream::connect(peer_address).await.unwrap();
                        if let Some(mut stream) = streamMatch{
                            let encoded: Vec<u8> = bincode::serialize(&msg).unwrap();
                            stream.write_all(&encoded).await.unwrap();
                            println!("sent BLE heartbeat to {}", &msg.to);
                            
                        }
                       
                    }
                }
                _ => {
                    let splitter = string.split(" ");
                    let args = splitter.collect::<Vec<&str>>();
                    let key = args[1];

                    match args[0] {
                        "write" => {
                            let value = &args[2];
                            let write_entry = KeyValue {
                                key: String::from(args[1]),
                                value: value.trim().parse().expect("expected int"),
                            };
                            seq_paxos.append(write_entry).expect("failed to append");
                            println!("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

                            println!("APPENDED {} to {}", key, value);
                            println!("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                        }

                        "read" => {
                            let log = seq_paxos.read_decided_suffix(0); //få alla decided index från och med index 0, alltså alla decided.
                            println!("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

                            println!("received log");
                            println!("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

                            let mut latest_value = 0;

                            match log {
                                Some(vector) => {
                                    // println!("found a vector");

                                    for LogEntry in vector.to_vec().iter() {
                                        // println!("found logentry");
                                        match LogEntry {
                                            Decided(KV) => {
                                                if key == KV.key {
                                                    // println!("Key: {} Value: {}", KV.key, KV.value);
                                                    latest_value = KV.value;
                                                }
                                            }

                                            _ => {
                                                println!("no such key has been found in the decided list");
                                            }
                                        }
                                    }   
                                    println!(
                                        "LATEST ENTRY for KEY: {} is VALUE: {}",
                                        key, latest_value
                                    );                               
                                }
                                _ => println!("You really shouldnt be here"),
                            }

                            
                        }

                        _ => {}
                    }
                }
            }
        }
    });
    loop {} //make sure the program keeps rolling after
}
