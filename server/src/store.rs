use crate::command::Command;
use async_std::net::SocketAddr;
use async_std::task::JoinHandle;
use std::collections::HashMap;
use time::{Duration, PrimitiveDateTime};

type ClientId = usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Str(String),
}

pub struct Expiration {
    pub expires_at: PrimitiveDateTime,
    pub handle: JoinHandle<()>,
}

impl Expiration {
    pub fn new(duration: Duration, handle: JoinHandle<()>) -> Expiration {
        Expiration {
            expires_at: PrimitiveDateTime::now() + duration,
            handle,
        }
    }
}

struct Entry {
    value: Value,
    expiration: Option<Expiration>,
    touched_at: PrimitiveDateTime,
}

impl Entry {
    fn new(value: Value) -> Entry {
        Entry {
            value,
            expiration: None,
            touched_at: PrimitiveDateTime::now(),
        }
    }

    fn set_expiration(&mut self, expiration: Expiration) {
        self.expiration = Some(expiration);
        self.touched_at = PrimitiveDateTime::now();
    }

    fn ttl(&self) -> Option<i64> {
        if let Some(exp) = &self.expiration {
            let ttl = exp.expires_at - PrimitiveDateTime::now();
            Some(ttl.whole_seconds())
        } else {
            None
        }
    }

    fn touched_at(&self) -> &PrimitiveDateTime {
        &self.touched_at
    }
}

pub struct Store {
    data: HashMap<String, Entry>,
    clients: HashMap<SocketAddr, ClientId>,
    next_client_id: ClientId,
}

impl Store {
    pub fn new() -> Store {
        Store {
            data: HashMap::new(),
            clients: HashMap::new(),
            next_client_id: 1,
        }
    }

    pub fn add_client(&mut self, addr: SocketAddr) -> ClientId {
        let client_id = self.next_client_id;
        self.next_client_id += 1;
        self.clients.insert(addr, client_id);
        client_id
    }

    pub fn remove_client(&mut self, addr: &SocketAddr) {
        self.clients.remove(addr);
    }

    pub fn set(&mut self, key: String, value: String, keep_ttl: bool) -> Option<()> {
        let value = if let Ok(int) = value.parse::<i64>() {
            Value::Int(int)
        } else {
            Value::Str(value)
        };

        let entry = if keep_ttl {
            let maybe_expiration = self.data.remove(&key).and_then(|entry| entry.expiration);
            let mut new_entry = Entry::new(value);
            if let Some(exp) = maybe_expiration {
                new_entry.set_expiration(exp);
            }
            new_entry
        } else {
            Entry::new(value)
        };
        self.data.insert(key, entry);

        Some(())
    }

    pub fn get(&self, key: &String) -> Option<&Value> {
        self.data.get(key).map(|entry| &entry.value)
    }

    pub fn remove(&mut self, key: &String) -> Option<()> {
        self.data.remove(key).map(|_| ())
    }

    pub fn expire(&mut self, key: &String, expiration: Expiration) -> Option<()> {
        if let Some(entry) = self.data.get_mut(key) {
            entry.set_expiration(expiration);
            Some(())
        } else {
            None
        }
    }

    pub fn ttl(&self, key: &String) -> TTL {
        if let Some(entry) = self.data.get(key) {
            if let Some(ttl) = entry.ttl() {
                TTL::Expires(ttl)
            } else {
                TTL::NoExpiration
            }
        } else {
            TTL::KeyNotFound
        }
    }

    pub fn last_touched(&self, key: &String) -> Option<&PrimitiveDateTime> {
        self.data.get(key).map(|entry| entry.touched_at()).or(None)
    }
}

pub enum TTL {
    NoExpiration,
    KeyNotFound,
    Expires(i64),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_get() {
        let mut store = Store::new();
        store.set("foo".to_owned(), "bar".to_owned(), false);
        store.set("a_num".to_owned(), "42".to_owned(), false);
        assert_eq!(
            Some(&Value::Str("bar".to_owned())),
            store.get(&"foo".to_owned())
        );
        assert_eq!(Some(&Value::Int(42)), store.get(&"a_num".to_owned()));
        assert_eq!(None, store.get(&"not_here".to_owned()));
    }
}
