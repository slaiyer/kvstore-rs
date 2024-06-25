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
    fs::{File, OpenOptions},
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
    wal: Option<PathBuf>,
}

/// Result wrapper type for KV store methods
pub type Result<T> = result::Result<T, KvStoreError>;

/// Methods on KV store
impl KvStore {
    /// Returns empty `DashMap` with default hasher
    #[must_use]
    pub fn new() -> Self {
        // TODO: scan current directory for WAL

        Self::default()
    }

    /// Constructs a new in-memory KV store by parsing on-disk write-ahead log (WAL)
    ///
    /// # Errors
    /// Returns `Err` if on-disk WAL read fails
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut store = Self::new();
        let wal = path.into().join("wa.log");
        store.wal = Some(wal.clone());

        match File::open(wal) {
            Err(e) => eprintln!("{e:?}"),
            Ok(wal) => store.wal_read(wal)?,
        };

        Ok(store)
    }

    fn wal_read(&self, wal: File) -> Result<()> {
        for line in io::BufReader::new(wal).lines() {
            println!("[wal] {}", self.wal_line_read(line)?);
        }

        Ok(())
    }

    fn wal_line_read(&self, line: result::Result<String, io::Error>) -> Result<String> {
        match line {
            Err(e) => Err(KvStoreError::FailedIo(e)),
            Ok(line) => Ok(self.wal_line_deserialize(&line)?),
        }
    }

    fn wal_line_deserialize(&self, line: &str) -> Result<String> {
        match serde_json::from_str(line) {
            Err(e) => Err(KvStoreError::DeserializeCommand(e)),
            Ok(cmd) => Ok(self.execute(cmd)?),
        }
    }

    /// Executes a command as an operation on the KV store
    ///
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

    /// TODO: Cache WAL file handle to avoid opening and closing file for each operation logged
    ///
    /// Records operations in write-ahead log (WAL) if WAL is provided
    ///
    /// # Errors
    /// Returns `Err` if `open` or `write_all` fail
    fn wal_write(&self, s: &str) -> Result<()> {
        match self.wal {
            Some(ref wal) => Ok(OpenOptions::new()
                .create(true)
                .append(true)
                .open(wal)?
                .write_all(s.as_bytes())?),
            _ => Ok(()),
        }
    }

    /// Inserts key-value pair into store
    ///
    /// # Errors
    /// Returns `Err` if on-disk WAL write fails
    pub fn set(&self, key: String, value: String) -> Result<()> {
        self.wal_write(&format!("set {key} {value}"))?;
        self.store.insert(key, value);

        Ok(())
    }

    /// Returns value for given key from store if present
    ///
    /// # Errors
    /// Returns `Err` if KV store read fails
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.store.get(&key) {
            Some(v) => Ok(Some(v.value().to_owned())),
            _ => Err(KvStoreError::FailedGet(key)),
        }
    }

    /// Removes key-value pair from store for given key
    ///
    /// # Errors
    /// Returns `Err` if on-disk WAL write fails
    pub fn remove(&self, key: String) -> Result<()> {
        self.wal_write(&format!("rm {key}"))?;
        match self.store.remove(&key) {
            None => Err(KvStoreError::FailedRm(key)),
            Some(_) => Ok(()),
        }
    }
}

/// Error wrapper for KV store methods
#[derive(Debug, Error)]
pub enum KvStoreError {
    /// Generic command deserializatio error wrapper
    #[error("Deserialization failure: {0}")]
    DeserializeCommand(#[from] serde_json::error::Error),
    /// Invalid/unsupported command
    #[error("Invalid command: {0}")]
    InvalidCommand(String),
    /// Invalid/unsupported command
    #[error("Missing command")]
    MissingCommand,
    /// Missing key for command
    #[error("Key not supplied: {0}")]
    MissingKey(String),
    /// Missing value for command
    #[error("Value not supplied: {0}")]
    MissingValue(String),
    /// Failed WAL I/O
    #[error("I/O failed on write-ahead log: {0}")]
    FailedIo(#[from] io::Error),
    /// Failed KV store read
    #[error("Key not found: {0}")]
    FailedGet(String),
    /// Failed KV store remove
    #[error("Key not found: {0}")]
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

/// Simple serializer for generating space-separated command representation for the WAL, mirroring the CLI input format
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

/// Simple deserializer for parsing space-separated command representation in the WAL, mirroring the CLI input format
impl<'de> Deserialize<'de> for Command {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let mut parts = s.split_ascii_whitespace();

        match parts.next() {
            None => Err(D::Error::custom(KvStoreError::MissingCommand)),
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
