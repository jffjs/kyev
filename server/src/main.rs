use async_std::{
    io::BufReader,
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    sync::{Arc, RwLock},
    task,
};
use std::convert::TryFrom;
use time::PrimitiveDateTime;

#[macro_use]
extern crate lazy_static;

use kyev::command::{self, Action, Command, CommandOpt};
use kyev::store::{self, Expiration, Store, TTL};
use kyev::transaction::Transaction;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

lazy_static! {
    static ref STORE: RwLock<Store> = RwLock::new(Store::new());
}

fn main() -> Result<()> {
    let fut = accept_loop("127.0.0.1:8080");
    println!("Listening on port 8080");
    task::block_on(fut)
}

async fn accept_loop(addr: impl ToSocketAddrs) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        let stream = stream?;
        let client_addr = stream.peer_addr()?;
        println!("Accepting from: {}", client_addr);
        let _handle = spawn_and_log_error(connection_loop(client_addr, stream));
    }
    Ok(())
}

fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            eprintln!("{}", e)
        }
    })
}

type WatchKey = (String, PrimitiveDateTime);

async fn connection_loop(client_addr: SocketAddr, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let mut reader = BufReader::new(&*stream);
    let mut string_buf = String::new();
    let mut transaction: Option<Transaction> = None;
    let mut watch: Vec<WatchKey> = Vec::new();
    let client_id = {
        let mut store = STORE.write().await;
        store.add_client(client_addr)
    };

    while let Ok(bytes_read) = reader.read_line(&mut string_buf).await {
        if bytes_read == 0 {
            break;
        }

        match resp::decode(&string_buf) {
            Ok(value) => {
                let response = match Command::from_resp(value) {
                    Ok(mut cmd) => match cmd.action() {
                        Action::ClientId => resp::integer(client_id as i64),
                        Action::Multi => {
                            if let None = transaction {
                                transaction = Some(Transaction::new());
                            }
                            resp::simple_string("OK")
                        }
                        Action::Exec => {
                            if let Some(trx) = transaction.take() {
                                let value = execute_transaction(trx, &watch).await;
                                watch.clear();
                                value
                            } else {
                                resp::Value::Null
                            }
                        }
                        Action::Discard => {
                            if transaction.is_some() {
                                transaction.take();
                                resp::simple_string("OK")
                            } else {
                                resp::Value::Null
                            }
                        }
                        Action::Watch => {
                            for key_to_watch in cmd
                                .args_mut()
                                .drain(..)
                                .map(|key| (key, PrimitiveDateTime::now()))
                            {
                                watch.push(key_to_watch);
                            }
                            resp::simple_string("OK")
                        }
                        Action::Unwatch => {
                            watch.clear();
                            resp::simple_string("OK")
                        }
                        _ => {
                            if let Some(mut trx) = transaction.take() {
                                trx.push(cmd);
                                transaction = Some(trx);
                                resp::simple_string("QUEUED")
                            } else {
                                if let Some(lock) = cmd.lock() {
                                    match lock {
                                        command::Lock::Read => {
                                            let store = STORE.read().await;
                                            execute_read_cmd(&store, cmd)
                                        }
                                        command::Lock::Write => {
                                            let mut store = STORE.write().await;
                                            execute_write_cmd(&mut store, cmd)
                                        }
                                    }
                                } else {
                                    execute_cmd(cmd)
                                }
                            }
                        }
                    },
                    Err(e) => {
                        let msg = format!("{}", e);
                        resp::error(msg.as_str())
                    }
                };
                let mut stream = &*stream;
                stream.write_all(resp::encode(&response).as_bytes()).await?;
                string_buf.clear();
            }
            Err(resp::Error::IncompleteRespError) => continue,
            _ => {
                println!("{}", string_buf);
                println!("Invalid resp!");
                string_buf.clear();
            }
        }
    }

    STORE.write().await.remove_client(&client_addr);
    println!("Client disconnected: {}", client_addr);

    Ok(())
}

async fn execute_transaction(mut trx: Transaction, watch: &Vec<WatchKey>) -> resp::Value {
    let mut store = STORE.write().await;

    for (key, watch_start) in watch.iter() {
        if let Some(last_touched) = store.last_touched(key) {
            if last_touched >= watch_start {
                return resp::Value::Null;
            }
        }
    }

    let results: Vec<resp::Value> = trx
        .drain_queue()
        .map(move |cmd| {
            if let Some(lock) = cmd.lock() {
                match lock {
                    command::Lock::Read => execute_read_cmd(&store, cmd),
                    command::Lock::Write => execute_write_cmd(&mut store, cmd),
                }
            } else {
                execute_cmd(cmd)
            }
        })
        .collect();

    resp::array(results)
}

fn execute_cmd(cmd: Command) -> resp::Value {
    use kyev::command::Action::*;
    match cmd.action() {
        Ping => {
            if let Some(arg) = cmd.args().first() {
                resp::bulk_string(&arg)
            } else {
                resp::simple_string("PONG")
            }
        }

        Echo => resp::bulk_string(&cmd.args().first().unwrap_or(&String::new())),
        _ => panic!("Command '{}' requires store access", cmd),
    }
}

fn execute_read_cmd(store: &Store, cmd: Command) -> resp::Value {
    use kyev::command::Action::*;

    match cmd.action() {
        Get => execute_get(store, cmd),
        Ttl => execute_ttl(store, cmd),
        _ => panic!("Command '{}' should be executed with write access", cmd),
    }
}

fn execute_write_cmd(store: &mut Store, cmd: Command) -> resp::Value {
    use kyev::command::Action::*;

    match cmd.action() {
        Set => execute_set(store, cmd),
        SetEx => execute_setex(store, cmd),
        SetNx => execute_setnx(store, cmd),
        Expire => execute_expire(store, cmd, false),
        PExpire => execute_expire(store, cmd, true),
        _ => panic!("Command '{}' should be executed with read access", cmd),
    }
}

async fn create_expiration_task(ttl: std::time::Duration, key: String) {
    task::sleep(ttl).await;
    let mut store = STORE.write().await;
    if let TTL::Expires(ttl) = store.ttl(&key) {
        if ttl > 0 {
            return;
        }
    }
    store.remove(&key);
}

fn execute_set(store: &mut Store, mut cmd: Command) -> resp::Value {
    let key: String;
    let val: String;
    {
        let mut drain = cmd.drain_args();
        key = drain.next().unwrap();
        val = drain.next().unwrap();
    }

    let mut maybe_ttl = None;
    let mut keep_ttl = false;
    let mut xx = false;
    let mut nx = false;
    for opt in cmd.opts().iter() {
        match opt {
            CommandOpt::SetEx(ttl_sec) => maybe_ttl = Some(ttl_sec * 1000),
            CommandOpt::SetPx(ttl_ms) => maybe_ttl = Some(*ttl_ms),
            CommandOpt::SetKeepTtl => keep_ttl = true,
            CommandOpt::SetXx => xx = true,
            CommandOpt::SetNx => nx = true,
            // _ => continue,
        };
    }

    if xx {
        if let Some(_) = store.get(&key) {
            store.set(key.clone(), val, keep_ttl);
            if let Some(ttl) = maybe_ttl {
                let join_handle = task::spawn(create_expiration_task(
                    std::time::Duration::from_millis(ttl),
                    key.clone(),
                ));
                store.expire(
                    &key,
                    Expiration::new(time::Duration::milliseconds(ttl as i64), join_handle),
                );
            }
            return resp::integer(1);
        } else {
            return resp::integer(0);
        }
    }

    if nx {
        if let None = store.get(&key) {
            store.set(key.clone(), val, keep_ttl);
            if let Some(ttl) = maybe_ttl {
                let join_handle = task::spawn(create_expiration_task(
                    std::time::Duration::from_millis(ttl),
                    key.clone(),
                ));
                store.expire(
                    &key,
                    Expiration::new(time::Duration::milliseconds(ttl as i64), join_handle),
                );
            }
            return resp::integer(1);
        } else {
            return resp::integer(0);
        }
    }

    store.set(key.clone(), val, keep_ttl);
    if let Some(ttl) = maybe_ttl {
        let join_handle = task::spawn(create_expiration_task(
            std::time::Duration::from_millis(ttl),
            key.clone(),
        ));
        store.expire(
            &key,
            Expiration::new(time::Duration::milliseconds(ttl as i64), join_handle),
        );
    }
    resp::simple_string("OK")
}

fn execute_setex(store: &mut Store, mut cmd: Command) -> resp::Value {
    let mut drain = cmd.drain_args();
    let key = drain.next().unwrap();
    let ttl = drain.next().unwrap().parse::<i64>().unwrap();
    let val = drain.next().unwrap();
    store.set(key.clone(), val, false);
    let join_handle = task::spawn(create_expiration_task(
        std::time::Duration::from_secs(ttl as u64),
        key.clone(),
    ));
    store.expire(
        &key,
        Expiration::new(time::Duration::seconds(ttl), join_handle),
    );
    resp::simple_string("OK")
}

fn execute_setnx(store: &mut Store, mut cmd: Command) -> resp::Value {
    let mut drain = cmd.drain_args();
    let key = drain.next().unwrap();
    if let Some(_) = store.get(&key) {
        resp::integer(0)
    } else {
        let val = drain.next().unwrap();
        store.set(key, val, false);
        resp::integer(1)
    }
}

fn execute_get(store: &Store, cmd: Command) -> resp::Value {
    let key = cmd.args().first().unwrap();
    let val = store.get(key);
    match val {
        Some(v) => match v {
            store::Value::Int(i) => resp::bulk_string(i.to_string().as_str()),
            store::Value::Str(s) => resp::bulk_string(s.as_str()),
        },
        None => resp::Value::Null,
    }
}

fn execute_expire(store: &mut Store, mut cmd: Command, as_ms: bool) -> resp::Value {
    let mut drain = cmd.drain_args();
    let key = drain.next().unwrap();
    let ttl = drain.next().unwrap().parse::<i64>().unwrap();

    if ttl < 0 {
        resp::integer(match store.remove(&key) {
            Some(_) => 1,
            None => 0,
        })
    } else {
        let duration = if as_ms {
            std::time::Duration::from_millis(ttl as u64)
        } else {
            std::time::Duration::from_secs(ttl as u64)
        };
        let join_handle = task::spawn(create_expiration_task(duration, key.clone()));
        if let Some(_) = store.expire(
            &key,
            Expiration::new(time::Duration::try_from(duration).unwrap(), join_handle),
        ) {
            resp::integer(1)
        } else {
            resp::integer(0)
        }
    }
}

fn execute_ttl(store: &Store, cmd: Command) -> resp::Value {
    let key = cmd.args().first().unwrap();
    resp::integer(match store.ttl(key) {
        TTL::Expires(ttl) => ttl,
        TTL::NoExpiration => -1,
        TTL::KeyNotFound => -2,
    })
}
