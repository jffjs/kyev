use async_std::{
    io::BufReader,
    net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs},
    prelude::*,
    task,
};
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

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

async fn connection_loop(client_addr: SocketAddr, stream: TcpStream) -> Result<()> {
    let stream = Arc::new(stream);
    let reader = BufReader::new(&*stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next().await {
        let mut line: String = line?;
        line.push('\n');
        let mut stream = &*stream;
        stream.write_all(line.as_bytes()).await?;
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
