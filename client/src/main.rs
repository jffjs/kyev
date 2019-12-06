use std::io::{prelude::*, stdin, stdout, BufRead, BufReader};
use std::net::{TcpStream, ToSocketAddrs};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    // let stream = TcpStream::connect("127.0.0.1:8080");
    // let (reader, mut writer) = (&stream, &stream);

    println!("foo");
    loop {
        write_prompt();
        let mut input = String::new();
        stdin().read_line(&mut input)?;
        print!("{}", input);
    }
}

fn write_prompt() {
    print!("127.0.0.1:8080> ");
    stdout().flush().unwrap();
}
