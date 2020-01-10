use crate::command::Command;
use std::collections::HashMap;

pub struct Store {
    data: HashMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(String),
}

impl Store {
    pub fn new() -> Store {
        Store {
            data: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Option<()> {
        let value = if let Ok(int) = value.parse::<i64>() {
            Value::Int(int)
        } else {
            Value::Str(value)
        };

        self.data.insert(key, value);

        Some(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    kind: StoreErrorKind,
    command: Option<Command>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreErrorKind {
    Unknown,
}
