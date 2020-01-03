use std::io::{prelude::*, stdin, stdout, BufRead, BufReader};
use std::net::{TcpStream, ToSocketAddrs};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    let stream = TcpStream::connect("127.0.0.1:8080").expect("Couldn't connect to server...");
    let (reader, mut writer) = (&stream, &stream);
    let mut buf_reader = BufReader::new(reader);

    loop {
        let mut input = String::new();
        let mut output = String::new();

        write_prompt();
        stdin().read_line(&mut input)?;

        writer.write(input.as_bytes())?;
        buf_reader.read_line(&mut output)?;
        print!("{}", output);
    }
}

fn write_prompt() {
    print!("127.0.0.1:8080> ");
    stdout().flush().unwrap();
}
