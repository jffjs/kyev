use resp;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Ping,
    Echo,
}

impl Action {
    pub fn parse(s: &String) -> Result<Action, ParseCommandError> {
        let s = s.to_lowercase();
        let s = s.as_str();
        if s == "ping" {
            Ok(Action::Ping)
        } else if s == "echo" {
            Ok(Action::Echo)
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

fn parse_ping(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(Action::Ping, &array, 1)?;
    let args = match array.iter().skip(1).next() {
        Some(value) => {
            let arg = value.to_string().map_err(|_| {
                ParseCommandError::new(ParseCommandErrorKind::InvalidArgs, Some(Action::Ping))
            })?;
            vec![arg]
        }
        None => vec![],
    };
    Ok(Command::new(Action::Ping, args))
}

fn parse_echo(array: &Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    use ParseCommandErrorKind::*;
    expect_max_args(Action::Echo, &array, 1)?;
    let arg = array
        .iter()
        .skip(1)
        .next()
        .ok_or(ParseCommandError::new(WrongNumberArgs, Some(Action::Echo)))?
        .to_string()
        .map_err(|_| ParseCommandError::new(InvalidArgs, Some(Action::Echo)))?;
    Ok(Command::new(Action::Echo, vec![arg]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        assert_eq!(
            Ok(Command::new(Action::Ping, vec![])),
            parse_ping(&vec![resp::bulk_string("PING")])
        );
        assert_eq!(
            Ok(Command::new(Action::Ping, vec!["hello".to_owned()])),
            parse_ping(&vec![resp::bulk_string("PING"), resp::bulk_string("hello")])
        );
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Ping)
            )),
            parse_ping(&vec![
                resp::bulk_string("PING"),
                resp::bulk_string("foo"),
                resp::bulk_string("bar")
            ])
        );
    }

    #[test]
    fn test_parse_echo() {
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Echo)
            )),
            parse_echo(&vec![resp::bulk_string("ECHO")])
        );
        assert_eq!(
            Ok(Command::new(Action::Echo, vec!["hello".to_owned()])),
            parse_echo(&vec![resp::bulk_string("ECHO"), resp::bulk_string("hello")])
        );
        assert_eq!(
            Err(ParseCommandError::new(
                ParseCommandErrorKind::WrongNumberArgs,
                Some(Action::Echo)
            )),
            parse_echo(&vec![
                resp::bulk_string("ECHO"),
                resp::bulk_string("foo"),
                resp::bulk_string("bar")
            ])
        );
    }
}
