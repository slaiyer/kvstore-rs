#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic, future_incompatible)]

//! Library code for key-value (KV) store implementation

use clap::Subcommand;
use dashmap::DashMap;
use serde::{
    de::{self, Error},
    ser::Serialize,
    Deserialize,
};
use std::{
    fs,
    io::{self, prelude::*},
    path::PathBuf,
    result,
};
use strum::{Display, EnumString};
use thiserror::Error;

/// Key-value (KV) store wrapper
#[derive(Default)]
pub struct KvStore {
    store: DashMap<String, String>,
    dir: PathBuf,
}

/// Result wrapper type for KV store methods
pub type Result<T> = result::Result<T, KvStoreError>;

/// Methods on KV store
impl KvStore {
    /// Returns empty `DashMap` with default hasher
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets working directory of KV store
    fn set_path(&mut self, path: PathBuf) {
        self.dir = path;
    }

    /// Constructs a new in-memory KV store by parsing on-disk write-ahead log (WAL)
    /// # Errors
    /// Returns `Err` if on-disk WAL read fails
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut store = Self::new();
        store.set_path(path.into());

        match fs::File::open(store.dir.join("wal.log")) {
            Err(e) => eprintln!("{e:?}"),
            Ok(wal) => {
                for line in io::BufReader::new(wal).lines() {
                    match line {
                        Err(_) => return Err(KvStoreError::FailedRead(io::Error::last_os_error())),
                        Ok(line) => match serde_json::from_str(line.as_str()) {
                            Err(e) => return Err(KvStoreError::DeserializeCommand(e)),
                            Ok(cmd) => match store.execute(cmd) {
                                Err(e) => {
                                    eprintln!("[wal] {e}");
                                    return Err(e);
                                }
                                Ok(s) => println!("[wal] {s}"),
                            },
                        },
                    };
                }
            }
        };

        Ok(store)
    }

    /// Executes a command as an operation on the KV store
    /// # Errors
    /// Return `Err` if operation failed
    pub fn execute(&self, cmd: Command) -> Result<String> {
        match cmd {
            Command::Get { key } => match self.get(key.clone()) {
                Err(e) => Err(e),
                Ok(value) => match value {
                    Some(v) => Ok(v),
                    _ => Err(KvStoreError::FailedGet(key)),
                },
            },
            Command::Set { key, value } => match self.set(key.clone(), value) {
                Err(e) => Err(e),
                _ => Ok(String::new()),
            },
            Command::Rm { key } => match self.remove(key.clone()) {
                Err(e) => Err(e),
                _ => Ok(String::new()),
            },
        }
    }

    /// Inserts key-value pair into store
    /// # Errors
    /// TODO: Returns `Err` if on-disk WAL write fails
    pub fn set(&self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);
        // TODO: write to WAL
        Ok(())
    }

    /// Returns value for given key from store if present
    /// # Errors
    /// Returns `Err` if KV store read fails
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(v) => Ok(Some(v.value().to_owned())),
            _ => Err(KvStoreError::FailedGet(key)),
        }
    }

    /// Removes key-value pair from store for given ke
    /// # Errors
    /// TODO: Returns `Err` if on-disk WAL write fails
    pub fn remove(&self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            Some(_) => Ok(()), // TODO: write to WAL
            _ => Err(KvStoreError::FailedRm(key)),
        }
    }
}

/// Error wrapper for KV store methods
#[derive(Debug, Error)]
pub enum KvStoreError {
    /// Generic command deserializatio error wrapper
    #[error(transparent)]
    DeserializeCommand(#[from] serde_json::error::Error),
    /// Invalid/unsupported command
    #[error("invalid command: {0}")]
    InvalidCommand(String),
    /// Missing key for command
    #[error("{0}: key not supplied")]
    MissingKey(String),
    /// Missing value for command
    #[error("{0}: value not supplied")]
    MissingValue(String),
    /// Failed WAL log read
    #[error("failed to read write-ahead log: {0:?}")]
    FailedRead(#[from] io::Error),
    /// Failed KV store read
    #[error("Key not found")]
    FailedGet(String),
    /// Failed KV store insert
    #[error("failed setting key: [{0}]")]
    FailedSet(String),
    /// Failed KV store remove
    #[error("Key not found")]
    FailedRm(String),
}

/// Supported operations on KV store
/// - Source of truth for CLI subcommands
/// - Specifies serde format for WAL read/write
#[derive(Debug, Display, EnumString, PartialEq, Subcommand)]
#[strum(serialize_all = "lowercase")]
pub enum Command {
    /// Get value by key
    Get {
        #[arg(required = true)]
        /// Key string
        key: String,
    },
    /// Set key-value pair by key
    Set {
        /// Key string
        #[arg(required = true)]
        key: String,
        /// Value string
        #[arg(required = true)]
        value: String,
    },
    /// Remove key-value pair by key
    Rm {
        /// Key string
        #[arg(required = true)]
        key: String,
    },
}

impl Serialize for Command {
    fn serialize<S>(&self, serializer: S) -> result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            cmd @ Self::Set { key, value } => {
                serializer.serialize_str(format!("{cmd} {key} {value}").as_str())
            }
            cmd @ (Self::Rm { key } | Self::Get { key }) => {
                serializer.serialize_str(format!("{cmd} {key}").as_str())
            }
        }
    }
}

impl<'de> Deserialize<'de> for Command {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let mut parts = s.split_ascii_whitespace();

        match parts.next() {
            None => Err(D::Error::custom(KvStoreError::MissingKey("[]".to_owned()))),
            Some(cmd) => match cmd {
                cmd @ "get" => match parts.next() {
                    None => Err(D::Error::custom(KvStoreError::MissingKey(cmd.to_owned()))),
                    Some(key) => Ok(Self::Get {
                        key: key.to_owned(),
                    }),
                },
                cmd @ "set" => match parts.next() {
                    None => Err(D::Error::custom(KvStoreError::MissingKey(cmd.to_owned()))),
                    Some(key) => match parts.next() {
                        None => Err(D::Error::custom(KvStoreError::MissingValue(cmd.to_owned()))),
                        Some(value) => Ok(Self::Set {
                            key: key.to_owned(),
                            value: value.to_owned(),
                        }),
                    },
                },
                cmd @ "rm" => match parts.next() {
                    None => Err(D::Error::custom(KvStoreError::MissingKey(cmd.to_owned()))),
                    Some(key) => Ok(Self::Rm {
                        key: key.to_owned(),
                    }),
                },
                cmd => Err(D::Error::custom(KvStoreError::InvalidCommand(
                    cmd.to_owned(),
                ))),
            },
        }
    }
}
