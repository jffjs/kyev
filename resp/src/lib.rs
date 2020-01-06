use std::convert::From;
use std::io::{BufRead, BufReader, Read};

static DELIMITER: &'static str = "\r\n";

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
    String(String),
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
        _ => Err(Error::InvalidRespError),
    }
}

fn decode_simple_string(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let mut buf = String::new();
    buf_reader.read_line(&mut buf)?;

    if buf.ends_with(DELIMITER) {
        Ok(Value::String(buf.trim_end().to_owned()))
    } else {
        Err(Error::IncompleteRespError)
    }
}

fn decode_bulk_string(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    let byte_count = read_int_with_clrf(buf_reader)?;

    dbg!(byte_count);
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

    Ok(Value::String(string.to_owned()))
}

fn decode_array(buf_reader: &mut BufReader<&[u8]>) -> Result<Value, Error> {
    unimplemented!()
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
    fn test_simple_string() {
        assert_eq!(Ok(Value::String("OK".to_owned())), decode("+OK\r\n"));
        assert_eq!(Ok(Value::String("HEY".to_owned())), decode("+HEY\r\n"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK\r"));
        assert_eq!(Err(Error::IncompleteRespError), decode("+OK\n"));
    }

    #[test]
    fn test_bulk_string() {
        assert_eq!(Ok(Value::String("OK".to_owned())), decode("$2\r\nOK\r\n"));
        assert_eq!(Ok(Value::String("HEY".to_owned())), decode("$3\r\nHEY\r\n"));
        assert_eq!(
            Ok(Value::String("HEY\r\nYA".to_owned())),
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
    fn test_arrays() {
        // assert_eq!(
        //     Ok(Value::Array(vec![Value::String("PING".to_owned())])),
        //     decode("*1\r\n$4\r\nPING\r\n")
        // )
    }
}
