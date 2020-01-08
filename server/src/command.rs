use resp;
use std::error::Error;
use std::fmt;

pub enum Action {
    Ping,
    Echo,
}

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
                let action = iter.next().ok_or(ParseCommandError { kind: IsEmpty })?;
                match action {
                    resp::Value::BulkString(cmd) | resp::Value::SimpleString(cmd) => {
                        if cmd.as_str() == "PING" {
                            Ok(Command::new(Action::Ping, vec![]))
                        } else if cmd.as_str() == "ECHO" {
                            let arg = iter
                                .next()
                                .ok_or(ParseCommandError { kind: InvalidArgs })?
                                .to_string()
                                .ok_or(ParseCommandError { kind: InvalidArgs })?;
                            Ok(Command::new(Action::Echo, vec![arg]))
                        } else {
                            Err(ParseCommandError {
                                kind: UnknownCommand,
                            })
                        }
                    }
                    _ => Err(ParseCommandError {
                        kind: UnknownCommand,
                    }),
                }
            }
            _ => Err(ParseCommandError { kind: MustBeArray }),
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseCommandErrorKind {
    MustBeArray,
    IsEmpty,
    UnknownCommand,
    InvalidArgs,
}

impl ParseCommandError {
    pub fn kind(&self) -> &ParseCommandErrorKind {
        &self.kind
    }
}

impl fmt::Display for ParseCommandError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.description().fmt(f)
    }
}

impl Error for ParseCommandError {
    fn description(&self) -> &str {
        use self::ParseCommandErrorKind::*;

        match self.kind {
            MustBeArray => "must be an array",
            IsEmpty => "array must contain at least one value",
            UnknownCommand => "command unknown",
            InvalidArgs => "invalid arguments for command",
        }
    }
}
