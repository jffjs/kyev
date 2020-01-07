use std::io::{prelude::*, stdin, stdout, BufReader};
use std::net::TcpStream;

extern crate clap;
use clap::{App, Arg};

use resp;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn main() -> Result<()> {
    let matches = App::new("kyev-cli")
        .version("0.1.0")
        .author("Jeff Smith")
        .arg(
            Arg::with_name("hostname")
                .short("h")
                .long("hostname")
                .value_name("HOSTNAME")
                .help("Host name of kyev server")
                .default_value("127.0.0.1"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .value_name("PORT")
                .help("Port of kyev server")
                .default_value("8080"),
        )
        .get_matches();

    let host = format!(
        "{}:{}",
        matches.value_of("hostname").unwrap_or_default(),
        matches.value_of("port").unwrap_or_default()
    );

    let stream = TcpStream::connect(&host).expect("Couldn't connect to server...");
    let (reader, mut writer) = (&stream, &stream);
    let mut buf_reader = BufReader::new(reader);

    loop {
        let mut input = String::new();
        let mut output = String::new();

        write_prompt(&host);
        stdin().read_line(&mut input)?;
        let resp = encode_resp(&input);

        writer.write(resp.as_bytes())?;
        buf_reader.read_line(&mut output)?;
        print!("{}", output);
    }
}

fn write_prompt(host: &str) {
    print!("{}> ", host);
    stdout().flush().unwrap();
}

fn encode_resp(input: &str) -> String {
    let array = input
        .split_whitespace()
        .map(|s| resp::bulk_string(s))
        .collect();
    resp::encode(&resp::array(array))
}
