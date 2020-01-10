use resp;
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Ping,
    Echo,
}

impl Action {
    pub fn from_str(s: &str) -> Result<Action, ParseCommandError> {
        let s = s.to_uppercase();
        let s = s.as_str();
        if s == "PING" {
            Ok(Action::Ping)
        } else if s == "ECHO" {
            Ok(Action::Echo)
        } else {
            Err(ParseCommandError::new(
                ParseCommandErrorKind::UnknownCommand,
                Some(s),
            ))
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
                let mut iter = array.iter();
                let action_resp = iter.next().ok_or(ParseCommandError::new(IsEmpty, None))?;
                match action_resp {
                    resp::Value::BulkString(cmd) => {
                        let cmd_str = cmd.as_str();
                        let action = Action::from_str(cmd_str)?;
                        match action {
                            Action::Ping => {
                                expect_max_args(cmd_str, &array, 1)?;
                                let args = match iter.next() {
                                    Some(value) => {
                                        let arg = value.to_string().map_err(|_| {
                                            ParseCommandError::new(InvalidArgs, Some(cmd_str))
                                        })?;
                                        vec![arg]
                                    }
                                    None => vec![],
                                };
                                Ok(Command::new(action, args))
                            }
                            Action::Echo => {
                                expect_max_args(cmd_str, &array, 1)?;
                                let arg = iter
                                    .next()
                                    .ok_or(ParseCommandError::new(WrongNumberArgs, Some(cmd_str)))?
                                    .to_string()
                                    .map_err(|_| {
                                        ParseCommandError::new(InvalidArgs, Some(cmd_str))
                                    })?;
                                Ok(Command::new(Action::Echo, vec![arg]))
                            }
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

fn expect_max_args(
    cmd_str: &str,
    v: &Vec<resp::Value>,
    max: usize,
) -> Result<(), ParseCommandError> {
    if v.len() > max + 1 {
        Err(ParseCommandError::new(
            ParseCommandErrorKind::WrongNumberArgs,
            Some(cmd_str),
        ))
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseCommandError {
    kind: ParseCommandErrorKind,
    command: Option<String>,
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
    pub fn new(kind: ParseCommandErrorKind, command: Option<&str>) -> ParseCommandError {
        ParseCommandError {
            kind,
            command: command.map(|c| c.to_owned()),
        }
    }

    pub fn kind(&self) -> &ParseCommandErrorKind {
        &self.kind
    }

    pub fn command(&self) -> &Option<String> {
        &self.command
    }
}

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::ParseCommandErrorKind::*;

        let empty_string = &String::default();
        let command = self
            .command()
            .as_ref()
            .unwrap_or(empty_string)
            .to_lowercase();

        match &self.kind {
            MustBeArray => "ERR must be an array".fmt(f),
            IsEmpty => "ERR array must contain at least one value".fmt(f),
            UnknownCommand => write!(f, "ERR Unknown or disabled command '{}'", command),
            InvalidArgs => "ERR invalid arguments for command".fmt(f),
            InvalidCommand => "ERR command values must be Bulk Strings".fmt(f),
            WrongNumberArgs => write!(f, "ERR wrong number of arguments for '{}' command", command),
        }
    }
}

fn parse_ping(cmd_str: &str, array: Vec<resp::Value>) -> Result<Command, ParseCommandError> {
    expect_max_args(cmd_str, &array, 1)?;
    let args = match array.iter().skip(1).next() {
        Some(value) => {
            let arg = value.to_string().map_err(|_| {
                ParseCommandError::new(ParseCommandErrorKind::InvalidArgs, Some(cmd_str))
            })?;
            vec![arg]
        }
        None => vec![],
    };
    Ok(Command::new(Action::Ping, args))
}
