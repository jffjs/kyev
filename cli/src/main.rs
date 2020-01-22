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
        while let Ok(bytes_read) = buf_reader.read_line(&mut output) {
            if bytes_read == 0 {
                break;
            }

            match resp::decode(&output) {
                Ok(value) => {
                    match value {
                        resp::Value::SimpleString(s) | resp::Value::BulkString(s) => {
                            println!("\"{}\"", s);
                        }
                        resp::Value::Error(e) => {
                            println!("{}", e);
                        }
                        resp::Value::Null => {
                            println!("(nil)");
                        }
                        resp::Value::Integer(i) => {
                            println!("(integer) {}", i);
                        }
                        _ => unimplemented!(),
                    }
                    output.clear();
                    break;
                }
                Err(resp::Error::IncompleteRespError) => continue,
                _ => {
                    println!("ERR invalid response");
                    output.clear();
                }
            }
        }
    }
}

fn write_prompt(host: &str) {
    print!("{}> ", host);
    stdout().flush().unwrap();
}

fn encode_resp(input: &str) -> String {
    let array = tokenize(input.trim_end())
        .iter()
        .map(|s| resp::bulk_string(s.as_str()))
        .collect();
    resp::encode(&resp::array(array))
}

fn tokenize(s: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = s.chars();

    let mut in_quote = false;
    let mut token = String::new();
    while let Some(c) = chars.next() {
        if c == '"' {
            in_quote = !in_quote;
            continue;
        }

        if c == ' ' && !in_quote && !token.is_empty() {
            tokens.push(token.clone());
            token.clear();
            continue;
        }

        token.push(c);
    }

    if !token.is_empty() {
        tokens.push(token);
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_input() {
        assert_eq!(vec!["PING".to_owned()], tokenize("PING"));
        assert_eq!(
            vec!["ECHO".to_owned(), "foo".to_owned()],
            tokenize("ECHO foo")
        );
        assert_eq!(
            vec!["ECHO".to_owned(), "foo".to_owned(), "bar".to_owned()],
            tokenize("ECHO foo bar")
        );
        assert_eq!(
            vec!["ECHO".to_owned(), "foo bar".to_owned()],
            tokenize("ECHO \"foo bar\"")
        );
    }
}
