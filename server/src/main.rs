use async_std::{
    io::BufReader,
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    sync::{Arc, RwLock},
    task,
};

#[macro_use]
extern crate lazy_static;

use kyev::command::Command;
use kyev::store::{self, Expiration, Store};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

lazy_static! {
    static ref STORE: RwLock<Store> = RwLock::new(Store::new());
}

fn main() -> Result<()> {
    let fut = accept_loop("127.0.0.1:8080");
    println!("Listening on port 8080");
    task::block_on(fut)
}

async fn execute_cmd(resp_val: resp::Value) -> String {
    use kyev::command::Action::*;

    match Command::from_resp(resp_val) {
        Ok(mut cmd) => match cmd.action() {
            Ping => resp::encode(
                &(if let Some(arg) = cmd.args().first() {
                    resp::bulk_string(&arg)
                } else {
                    resp::simple_string("PONG")
                }),
            ),
            Echo => resp::encode(&resp::bulk_string(
                &cmd.args().first().unwrap_or(&String::new()),
            )),
            Set => {
                let mut store = STORE.write().await;
                let mut drain = cmd.drain_args();
                let key = drain.next().unwrap();
                let val = drain.next().unwrap();
                store.set(key, val);
                resp::encode(&resp::simple_string("OK"))
            }
            Get => {
                // let store = store_lock.read().await;
                let store = STORE.read().await;
                let key = cmd.args().first().unwrap();
                let val = store.get(key);
                resp::encode(&match val {
                    Some(v) => match v {
                        store::Value::Int(i) => resp::bulk_string(i.to_string().as_str()),
                        store::Value::Str(s) => resp::bulk_string(s.as_str()),
                    },
                    None => resp::Value::Null,
                })
            }
            Expire => {
                let mut drain = cmd.drain_args();
                let key = drain.next().unwrap();
                let ttl = drain.next().unwrap().parse::<i64>().unwrap();
                let join_handle = task::spawn(create_expiration_task(ttl as u64, key.clone()));
                let mut store = STORE.write().await;
                store.expire(
                    &key,
                    Expiration::new(time::Duration::seconds(ttl), join_handle),
                );
                resp::encode(&resp::integer(1))
            }
        },
        Err(e) => {
            let msg = format!("{}", e);
            resp::encode(&resp::error(msg.as_str()))
        }
    }
}

async fn create_expiration_task(ttl: u64, key: String) {
    let timer = task::sleep(std::time::Duration::from_secs(ttl));
    timer.await;
    let mut store = STORE.write().await;
    store.remove(&key);
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

async fn connection_loop(client_addr: SocketAddr, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let mut reader = BufReader::new(&*stream);
    let mut string_buf = String::new();

    while let Ok(bytes_read) = reader.read_line(&mut string_buf).await {
        if bytes_read == 0 {
            break;
        }

        match resp::decode(&string_buf) {
            Ok(value) => {
                let response = execute_cmd(value).await;
                let mut stream = &*stream;
                stream.write_all(response.as_bytes()).await?;
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

    println!("Client disconnected: {}", client_addr);

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
