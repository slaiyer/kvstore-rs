#![deny(missing_docs)]
#![warn(clippy::all, clippy::pedantic, future_incompatible)]

//! Library code for key-value (KV) store implementation

use clap::Subcommand;
use dashmap::DashMap;
use serde::{
    de::{self, Deserializer, SeqAccess, Visitor},
    Deserialize, Serialize,
};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, prelude::*},
    path::{Path, PathBuf},
    result,
};
use strum::{Display, EnumString};
use thiserror::Error;

/// Write-ahead log file name
const WAL: &str = "wa.log";

/// Key-value (KV) store wrapper
pub struct KvStore {
    store: DashMap<String, String>,
    wal_handle: File,
}

/// Result wrapper type for KV store methods
pub type Result<T> = result::Result<T, KvStoreError>;

/// Methods on KV store
impl KvStore {
    /// Constructs a new in-memory KV store by parsing on-disk write-ahead log (WAL)
    ///
    /// # Errors
    /// Returns `Err` if WAL move, open, or read fails
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let wal_path = path.into().join(WAL);
        let old_wal_exists = wal_path.exists() && wal_path.is_file();
        let mut wal_path_moved = PathBuf::new();

        // Move existing WAL if it exists
        if old_wal_exists {
            wal_path_moved = Self::wal_old_move(&wal_path)?;
        }

        // Instantiate KV store with new WAL file handle
        let store = Self {
            store: DashMap::new(),
            wal_handle: Self::wal_new_open(&wal_path)?,
        };

        // Load old WAL if it exists
        if old_wal_exists {
            if let Err(e) = store.wal_old_load(&wal_path_moved) {
                // Undo old WAL move if load fails
                eprintln!("Failed to load old WAL: {e}");
                fs::rename(wal_path_moved, wal_path).map_err(KvStoreError::FailedWalRestore)?;
                return Err(e);
            }
        }

        Ok(store)
    }

    /// Moves existing WAL and returns its new path in an output parameter
    fn wal_old_move(wal_path: &Path) -> Result<PathBuf> {
        let wal_path_moved = {
            let mut ext = wal_path
                .extension()
                .ok_or(KvStoreError::InvalidWalFileName)?
                .to_os_string();
            ext.push(".old");
            let mut tmp = PathBuf::from(wal_path);
            tmp.set_extension(ext);
            tmp
        };

        fs::rename(wal_path, &wal_path_moved).map_err(KvStoreError::FailedWalRename)?;

        Ok(wal_path_moved)
    }

    fn wal_new_open(wal_path: &Path) -> Result<File> {
        OpenOptions::new()
            .truncate(true)
            .create(true)
            .write(true)
            .open(wal_path)
            .map_err(KvStoreError::FailedWalOpen)
    }

    fn wal_old_load(&self, wal_path: &Path) -> Result<()> {
        let wal = File::open(wal_path).map_err(KvStoreError::FailedOldWalOpen)?;
        self.wal_read(wal)?;

        // Delete old WAL if load succeeds
        if let Err(e) = fs::remove_file(wal_path) {
            eprintln!("Failed to remove moved old WAL: {e}");
        }

        Ok(())
    }

    fn wal_read(&self, wal: File) -> Result<()> {
        for line_result in io::BufReader::new(wal).lines() {
            // TODO: actually load WAL contents in memory?
            println!("{}", self.wal_line_read(line_result)?);
        }

        Ok(())
    }

    fn wal_line_read(&self, line_result: result::Result<String, io::Error>) -> Result<String> {
        match line_result {
            Err(e) => Err(KvStoreError::FailedWalLineRead(e)),
            Ok(line) => Ok(self.wal_line_deserialize(&line)?),
        }
    }

    fn wal_line_deserialize(&self, line: &str) -> Result<String> {
        match serde_json::from_str(&format!("[\"{}\"]", line.replace(' ', "\",\""))) {
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
                    _ => Ok(String::new()),
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

    /// Records operations in write-ahead log (WAL) if WAL is provided
    ///
    /// # Errors
    /// Returns `Err` if `open` or `write_all` fail
    fn wal_write(&self, s: &str) -> Result<()> {
        let s = format!("{s}\n");
        (&self.wal_handle)
            .write_all(s.as_bytes())
            .map_err(KvStoreError::FailedWalWrite)
    }

    /// Inserts key-value pair into store
    ///
    /// # Errors
    /// Returns `Err` if on-disk WAL write fails
    pub fn set(&self, key: String, value: String) -> Result<()> {
        // TODO: Use serde to serialize command

        self.wal_write(&format!("set {key} {value}"))?;
        self.store.insert(key, value);

        Ok(())
    }

    /// Returns value for given key from store if present
    ///
    /// # Errors
    /// Returns `Err` if KV store read fails
    pub fn get(&self, key: impl Into<String>) -> Result<Option<String>> {
        let key = key.into();
        if let Some(v) = self.store.get(&key) {
            Ok(Some(v.value().to_owned()))
        } else {
            println!("Key not found: {key}");
            Ok(None)
        }
    }

    /// Removes key-value pair from store for given key
    ///
    /// # Errors
    /// Returns `Err` if on-disk WAL write fails
    pub fn remove(&self, key: String) -> Result<()> {
        // TODO: Use serde to serialize command

        self.wal_write(&format!("rm {key}"))?;
        match self.store.remove(&key) {
            None => Err(KvStoreError::FailedRm(key)),
            Some(_) => Ok(()),
        }
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        println!("Flushing buffers...");
        if let Err(e) = self.wal_handle.flush() {
            eprintln!("Failed to flush buffer to WAL: {e}");
        }

        println!("Syncing to disk...");
        if let Err(e) = self.wal_handle.sync_all() {
            eprintln!("Failed to sync all to WAL: {e}");
        }
    }
}

/// Error wrapper for KV store methods
#[derive(Debug, Error)]
pub enum KvStoreError {
    /// Unknown current working directory
    #[error("Current working directory could not be determined")]
    UnknownCwd(io::Error),
    /// Unexpected WAL file name
    #[error("WAL does not have a file name extension")]
    InvalidWalFileName,
    /// Failed old WAL rename
    #[error("Failed to rename old WAL: {0}")]
    FailedWalRename(io::Error),
    /// Failed old WAL restore
    #[error("Failed to restore old WAL: {0}")]
    FailedWalRestore(io::Error),
    /// Failed new WAL open
    #[error("Failed to open new WAL: {0}")]
    FailedWalOpen(io::Error),
    /// Failed old WAL open
    #[error("Failed to open old WAL: {0}")]
    FailedOldWalOpen(io::Error),
    /// Failed line read from WAL
    #[error("Failed reading line from write-ahead log: {0}")]
    FailedWalLineRead(io::Error),
    /// Failed WAL write
    #[error("Failed to write to WAL: {0}")]
    FailedWalWrite(io::Error),
    /// Generic command deserialization error wrapper
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
/// TODO: Reconcile serializer with deserializer
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

struct CommandVisitor;

impl<'de> Visitor<'de> for CommandVisitor {
    type Value = Command;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("space separated string with subcommand and arguments")
    }

    fn visit_seq<V>(self, mut seq: V) -> result::Result<Self::Value, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let command: String = seq
            .next_element()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;

        match command.as_str() {
            "set" => {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let value = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Ok(Command::Set { key, value })
            }
            "rm" => {
                let key = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                Ok(Command::Rm { key })
            }
            _ => Err(de::Error::unknown_variant(&command, &["set", "rm"])),
        }
    }
}

impl<'de> Deserialize<'de> for Command {
    fn deserialize<D>(deserializer: D) -> result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(CommandVisitor)
    }
}
