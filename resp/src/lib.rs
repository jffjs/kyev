use std::convert::From;
use std::io::{BufRead, BufReader, Read};

static DELIMITER: &str = "\r\n";

#[derive(Debug, PartialEq)]
pub enum Error {
    IncompleteRespError,
    InvalidRespError,
}

impl From<std::io::Error> for Error {
    fn from(_: std::io::Error) -> Self {
        Error::IncompleteRespError
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(_: std::num::ParseIntError) -> Self {
        Error::InvalidRespError
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_: std::str::Utf8Error) -> Self {
        Error::InvalidRespError
    }
}

#[derive(Debug, PartialEq)]
pub enum Value {
    Array(Vec<Value>),
    SimpleString(String),
    BulkString(String),
    Error(String),
    Integer(i64),
}

impl Value {
    pub fn to_string(&self) -> Option<String> {
        use Value::*;

        match self {
            SimpleString(s) | BulkString(s) => Some(s.to_string()),
            _ => None,
        }
    }
}

pub fn array(a: Vec<Value>) -> Value {
    Value::Array(a)
}

pub fn simple_string(s: &str) -> Value {
    Value::SimpleString(s.to_owned())
}

pub fn bulk_string(s: &str) -> Value {
    Value::BulkString(s.to_owned())
}

pub fn error(s: &str) -> Value {
    Value::Error(s.to_owned())
}

pub fn integer(i: i64) -> Value {
    Value::Integer(i)
}

pub fn encode(value: &Value) -> String {
    match value {
        Value::SimpleString(_) => encode_simple_string(value),
        Value::BulkString(_) => encode_bulk_string(value),
        Value::Array(_) => encode_array(value),
        Value::Error(_) => encode_error(value),
        Value::Integer(_) => encode_integer(value),
    }
}

fn encode_simple_string(value: &Value) -> String {
    match value {
        Value::SimpleString(s) => format!("+{}\r\n", s),
        _ => panic!("Must be called with Value::SimpleString"),
    }
}

fn encode_error(value: &Value) -> String {
    match value {
        Value::Error(s) => format!("-{}\r\n", s),
        _ => panic!("Must be called with Value::Error"),
    }
}

fn encode_bulk_string(value: &Value) -> String {
    match value {
        Value::BulkString(s) => {
            let byte_count = s.bytes().len();
            format!("${}\r\n{}\r\n", byte_count, s)
        }
        _ => panic!("Must be called with Value::BulkString"),
    }
}

fn encode_integer(value: &Value) -> String {
    match value {
        Value::Integer(i) => format!(":{}\r\n", i),
        _ => panic!("Must be called with Value::Integer"),
    }
}

fn encode_array(value: &Value) -> String {
    match value {
        Value::Array(array) => {
            let mut string_buf = String::new();

            for value in array.iter() {
                string_buf.push_str(&encode(&value));
            }

            format!("*{}\r\n{}", array.len(), string_buf)
        }
        _ => panic!("Must be called with Value::Array"),
    }
}

pub fn decode(s: &str) -> Result<Value, Error> {
    let mut buf_reader = BufReader::new(s.as_bytes());
    do_decode(&mut buf_reader)
}

fn do_decode(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let mut buf = vec![0; 1];
    buf_reader.read_exact(&mut buf)?;
    match buf[0] {
        b'+' => decode_simple_string(buf_reader),
        b'$' => decode_bulk_string(buf_reader),
        b'*' => decode_array(buf_reader),
        b'-' => decode_error(buf_reader),
        b':' => decode_integer(buf_reader),
        _ => Err(Error::InvalidRespError),
    }
}

fn decode_simple_string(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf)?;

    if buf.ends_with(DELIMITER) {
        Ok(Value::SimpleString(buf.trim_end().to_owned()))
    } else {
        Err(Error::IncompleteRespError)
    }
}

fn decode_error(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf)?;

    if buf.ends_with(DELIMITER) {
        Ok(Value::Error(buf.trim_end().to_owned()))
    } else {
        Err(Error::IncompleteRespError)
    }
}

fn decode_bulk_string(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let byte_count = read_int_with_clrf(buf_reader)?;

    let mut buf = vec![0; byte_count];
    buf_reader.read_exact(&mut buf)?;
    let string = std::str::from_utf8(&buf)?;
    if string.len() != byte_count {
        return Err(Error::IncompleteRespError);
    }

    let mut buf = vec![0; DELIMITER.len()];
    buf_reader.read_exact(&mut buf)?;
    let closing_delimiter = std::str::from_utf8(&buf)?;
    if closing_delimiter != DELIMITER {
        return Err(Error::IncompleteRespError);
    }

    Ok(Value::BulkString(string.to_owned()))
}

fn decode_integer(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf)?;

    if buf.ends_with(DELIMITER) {
        Ok(Value::Integer(buf.trim_end().parse::<i64>()?))
    } else {
        Err(Error::IncompleteRespError)
    }
}

fn decode_array(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let element_count = read_int_with_clrf(buf_reader)?;

    let mut resp_array = Vec::with_capacity(element_count);

    for _ in 0..element_count {
        let value = do_decode(buf_reader)?;
        resp_array.push(value);
    }

    Ok(Value::Array(resp_array))
}

fn read_int_with_clrf(buf_reader: &mut BufReader<&[u8]>) -> Result<usize, Error> {
    let mut int_with_clrf = String::new();
    buf_reader.read_line(&mut int_with_clrf)?;

    if int_with_clrf.ends_with(DELIMITER) {
        let int = int_with_clrf.trim_end().parse::<usize>()?;
        Ok(int)
    } else {
        Err(Error::IncompleteRespError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        assert_eq!(
            "+OK\r\n".to_owned(),
            encode(&Value::SimpleString("OK".to_owned()))
        );
        assert_eq!(
            "+HEY\r\n".to_owned(),
            encode(&Value::SimpleString("HEY".to_owned()))
        );
        assert_eq!(
            "+What's up\r\n".to_owned(),
            encode(&Value::SimpleString("What's up".to_owned()))
        );
    }

    #[test]
    fn test_encode_bulk_string() {
        assert_eq!(
            "$2\r\nOK\r\n".to_owned(),
            encode(&Value::BulkString("OK".to_owned()))
        );
        assert_eq!(
            "$3\r\nHEY\r\n".to_owned(),
            encode(&Value::BulkString("HEY".to_owned()))
        );
        assert_eq!(
            "$7\r\nHEY\r\nYA\r\n".to_owned(),
            encode(&Value::BulkString("HEY\r\nYA".to_owned()))
        );
    }

    #[test]
    fn test_encode_array() {
        assert_eq!(
            "*1\r\n$4\r\nPING\r\n".to_owned(),
            encode(&Value::Array(vec![Value::BulkString("PING".to_owned())])),
        );

        assert_eq!(
            "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n".to_owned(),
            encode(&Value::Array(vec![
                Value::BulkString("ECHO".to_owned()),
                Value::BulkString("hey".to_owned())
            ])),
        );
    }

    #[test]
    fn test_encode_errors() {
        assert_eq!(
            "-ERR unknown command\r\n",
            encode(&Value::Error("ERR unknown command".to_owned()))
        );
    }

    #[test]
    fn test_encode_integers() {
        assert_eq!(":10\r\n", encode(&Value::Integer(10)));
    }

    #[test]
    fn test_decode_simple_string() {
        assert_eq!(Ok(Value::SimpleString("OK".to_owned())), decode("+OK\r\n"));
        assert_eq!(
            Ok(Value::SimpleString("HEY".to_owned())),
            decode("+HEY\r\n")
        );
        assert_eq!(Err(Error::IncompleteRespError), decode("+"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK\r"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK\n"));
    }

    #[test]
    fn test_decode_bulk_string() {
        assert_eq!(
            Ok(Value::BulkString("OK".to_owned())),
            decode("$2\r\nOK\r\n")
        );
        assert_eq!(
            Ok(Value::BulkString("HEY".to_owned())),
            decode("$3\r\nHEY\r\n")
        );
        assert_eq!(
            Ok(Value::BulkString("HEY\r\nYA".to_owned())),
            decode("$7\r\nHEY\r\nYA\r\n")
        );
        assert_eq!(Err(Error::IncompleteRespError), decode("$"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$2"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$2\r"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$2\r\n"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$2\r\nOK"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$2\r\nOK\r"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$8\r\nOK\r\nWAIT"));
        assert_eq!(Err(Error::IncompleteRespError), decode("$3\r\nOK\r\n"));
    }

    #[test]
    fn test_decode_arrays() {
        assert_eq!(
            Ok(Value::Array(vec![Value::BulkString("PING".to_owned())])),
            decode("*1\r\n$4\r\nPING\r\n")
        );

        assert_eq!(
            Ok(Value::Array(vec![
                Value::BulkString("ECHO".to_owned()),
                Value::BulkString("hey".to_owned())
            ])),
            decode("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
        );

        assert_eq!(Err(Error::IncompleteRespError), decode("*"));
        assert_eq!(Err(Error::IncompleteRespError), decode("*1"));
        assert_eq!(Err(Error::IncompleteRespError), decode("*1\r\n"));
        assert_eq!(Err(Error::IncompleteRespError), decode("*1\r\n$4"));
        assert_eq!(
            Err(Error::IncompleteRespError),
            decode("*2\r\n$4\r\nECHO\r\n")
        );
    }

    #[test]
    fn test_decode_errors() {
        assert_eq!(
            Ok(Value::Error("ERR unknown command".to_owned())),
            decode("-ERR unknown command\r\n")
        );
        assert_eq!(
            Err(Error::IncompleteRespError),
            decode("-ERR unknown command")
        );
    }

    #[test]
    fn test_decode_integers() {
        assert_eq!(Ok(Value::Integer(10)), decode(":10\r\n"));
        assert_eq!(Err(Error::IncompleteRespError), decode(":10"));
        assert_eq!(Err(Error::InvalidRespError), decode(":foo\r\n"));
    }
}
