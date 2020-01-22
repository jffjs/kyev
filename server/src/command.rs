use resp;
use std::fmt;

#[macro_export]
macro_rules! cmd {
    ($( $x:expr ),* ) => {
        {
            let mut temp_vec = Vec::new();
            $(
                temp_vec.push(resp::bulk_string($x));
            )*
            temp_vec
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Ping,
    Echo,
    Set,
    SetEx,
    Get,
    Expire,
    Ttl,
}

impl Action {
    pub fn parse(s: &String) -> Result<Action, ParseCommandError> {
        let s = s.to_lowercase();
        let s = s.as_str();
        if s == "ping" {
            Ok(Action::Ping)
        } else if s == "echo" {
            Ok(Action::Echo)
        } else if s == "set" {
            Ok(Action::Set)
        } else if s == "setex" {
            Ok(Action::SetEx)
        } else if s == "get" {
            Ok(Action::Get)
        } else if s == "expire" {
            Ok(Action::Expire)
        } else if s == "ttl" {
            Ok(Action::Ttl)
        } else {
            Err(ParseCommandError::new_with_context(
                ParseCommandErrorKind::UnknownCommand,
                None,
                s.to_owned(),
            ))
        }
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use Action::*;
        match self {
            Ping => "ping".fmt(f),
            Echo => "echo".fmt(f),
            Set => "set".fmt(f),
            SetEx => "setex".fmt(f),
            Get => "get".fmt(f),
            Expire => "expire".fmt(f),
            Ttl => "ttl".fmt(f),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command {
    action: Action,
    args: Vec<String>,
}

impl Command {
    pub fn new(action: Action, args: Vec<String>) -> Command {
        Command { action, args }
    }

    pub fn from_resp(resp_value: resp::Value) -> Result<Command, ParseCommandError> {
        use self::ParseCommandErrorKind::*;

        match resp_value {
            resp::Value::Array(array) => {
                let action_resp = array.first().ok_or(ParseCommandError::new(IsEmpty, None))?;
                match action_resp {
                    resp::Value::BulkString(cmd) => {
                        let action = Action::parse(cmd)?;
                        match action {
                            Action::Ping => parse_ping(&array),
                            Action::Echo => parse_echo(&array),
                            Action::Set => parse_set(&array),
                            Action::SetEx => parse_setex(&array),
                            Action::Get => parse_get(&array),
                            Action::Expire => parse_expire(&array),
                            Action::Ttl => parse_ttl(&array),
                        }
                    }
                    _ => Err(ParseCommandError::new(InvalidCommand, None)),
                }
            }
            _ => Err(ParseCommandError::new(MustBeArray, None)),
        }
    }

    pub fn action(&self) -> &Action {
        &self.action
    }

    pub fn args(&self) -> &Vec<String> {
        &self.args
    }

    pub fn drain_args(&mut self) -> std::vec::Drain<String> {
        self.args.drain(..)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCommandError {
    kind: ParseCommandErrorKind,
    action: Option<Action>,
    other_context: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseCommandErrorKind {
    MustBeArray,
    IsEmpty,
    UnknownCommand,
    InvalidArgs,
    InvalidCommand,
    WrongNumberArgs,
}

impl ParseCommandError {
    pub fn new(kind: ParseCommandErrorKind, action: Option<Action>) -> ParseCommandError {
        ParseCommandError {
            kind,
            action,
            other_context: None,
        }
    }

    pub fn new_with_context(
        kind: ParseCommandErrorKind,
        action: Option<Action>,
        other_context: String,
    ) -> ParseCommandError {
        ParseCommandError {
            kind,
            action,
            other_context: Some(other_context),
        }
    }

    pub fn kind(&self) -> &ParseCommandErrorKind {
        &self.kind
    }

    pub fn action(&self) -> &Option<Action> {
        &self.action
    }
}

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ParseCommandErrorKind::*;

        match &self.kind {
            MustBeArray => "ERR must be an array".fmt(f),
            IsEmpty => "ERR array must contain at least one value".fmt(f),
            UnknownCommand => write!(
                f,
                "ERR Unknown or disabled command '{}'",
                self.other_context.as_ref().unwrap()
            ),
            InvalidArgs => "ERR invalid arguments for command".fmt(f),
            InvalidCommand => "ERR command values must be Bulk Strings".fmt(f),
            WrongNumberArgs => write!(
                f,
                "ERR wrong number of arguments for '{}' command",
                self.action.as_ref().unwrap()
            ),
        }
    }
}

fn expect_max_args(
    action: Action,
    v: &Vec<resp::Value>,
    max: usize,
) -> Result<(), ParseCommandError> {
    if v.len() > max + 1 {
        Err(ParseCommandError::new(
            ParseCommandErrorKind::WrongNumberArgs,
            Some(action),
        ))
    } else {
        Ok(())
    }
}

fn next_arg<'a, I>(mut iter: I, action: Action) -> Result<String, ParseCommandError>
where
    I: Iterator<Item = &'a resp::Value>,
{
    iter.next()
        .ok_or(ParseCommandError::new(
            ParseCommandErrorKind::WrongNumberArgs,
            Some(action),
        ))?
        .to_string()
        .map_err(|_| ParseCommandError::new(ParseCommandErrorKind::InvalidArgs, Some(action)))
}

fn parse_ping(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Ping, &array, 1)?;
    let args = next_arg(array.iter().skip(1), Action::Ping)
        .map(|arg| vec![arg])
        .or(Ok(vec![]))?;

    Ok(Command::new(Action::Ping, args))
}

fn parse_echo(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Echo, &array, 1)?;
    let arg = next_arg(array.iter().skip(1), Action::Echo)?;
    Ok(Command::new(Action::Echo, vec![arg]))
}

fn parse_set(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    let mut iter = array.iter().skip(1);
    let key = next_arg(&mut iter, Action::Set)?;
    let val = next_arg(&mut iter, Action::Set)?;

    Ok(Command::new(Action::Set, vec![key, val]))
}

fn parse_setex(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    let action = Action::SetEx;
    expect_max_args(action, &array, 3)?;
    let mut iter = array.iter().skip(1);
    Ok(Command::new(
        action,
        vec![
            next_arg(&mut iter, action)?,
            next_arg(&mut iter, action)?,
            next_arg(&mut iter, action)?,
        ],
    ))
}

fn parse_get(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Echo, &array, 1)?;
    let mut iter = array.iter().skip(1);
    let key = next_arg(&mut iter, Action::Get)?;

    Ok(Command::new(Action::Get, vec![key]))
}

fn parse_expire(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Expire, &array, 2)?;
    let mut iter = array.iter().skip(1);
    let key = next_arg(&mut iter, Action::Expire)?;
    let ttl = next_arg(&mut iter, Action::Expire)?;

    Ok(Command::new(Action::Expire, vec![key, ttl]))
}

fn parse_ttl(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Ttl, &array, 1)?;
    let key = next_arg(array.iter().skip(1), Action::Ttl)?;
    Ok(Command::new(Action::Ttl, vec![key]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        assert_eq!(
            Ok(Command::new(Action::Ping, vec![])),
            parse_ping(&cmd!["PING"])
        );
        assert_eq!(
            Ok(Command::new(Action::Ping, vec!["hello".to_owned()])),
            parse_ping(&cmd!["PING", "hello"])
        );
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Ping)
            )),
            parse_ping(&cmd!["PING", "foo", "bar"])
        );
    }

    #[test]
    fn test_parse_echo() {
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Echo)
            )),
            parse_echo(&cmd!["ECHO"])
        );
        assert_eq!(
            Ok(Command::new(Action::Echo, vec!["hello".to_owned()])),
            parse_echo(&cmd!["ECHO", "hello"])
        );
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Echo)
            )),
            parse_echo(&cmd!["ECHO", "foo", "bar"])
        );
    }

    #[test]
    fn test_parse_set() {
        assert_eq!(
            Ok(Command::new(
                Action::Set,
                vec!["foo".to_owned(), "bar".to_owned()]
            )),
            parse_set(&cmd!["SET", "foo", "bar"])
        );
    }

    #[test]
    fn test_parse_get() {
        assert_eq!(
            Ok(Command::new(Action::Get, vec!["foo".to_owned()])),
            parse_get(&cmd!["GET", "foo"])
        );
    }

    #[test]
    fn test_parse_expire() {
        assert_eq!(
            Ok(Command::new(
                Action::Expire,
                vec!["foo".to_owned(), "5".to_owned()]
            )),
            parse_expire(&cmd!["EXPIRE", "foo", "5"])
        );
    }

    #[test]
    fn test_parse_ttl() {
        assert_eq!(
            Ok(Command::new(Action::Ttl, vec!["foo".to_owned()])),
            parse_ttl(&cmd!["TTL", "foo"])
        );
    }

    #[test]
    fn test_setex() {
        assert_eq!(
            Ok(Command::new(
                Action::SetEx,
                vec!["foo".to_owned(), "10".to_owned(), "bar".to_owned()]
            )),
            parse_setex(&cmd!["SETEX", "foo", "10", "bar"])
        );
    }
}
