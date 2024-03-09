use serde;
use serde::Deserialize;
use serde::Serialize;
use serde_json;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::str;
use std::u64;

pub type Result<T> = std::result::Result<T, KvError>;

#[derive(Debug)]
pub enum KvError {
    WriteError,
    OpenError,
    NoLogPathError,
    RemoveError(String),
    ReadLogError,
    InvalidLogCommand,
    SerializationError,
}

pub struct KvStore<'a> {
    store: HashMap<String, CommandBuffer>,
    log_path: PathBuf,
    append_handle: File,
    log_size: usize,
    number_of_writes: u64,
    path: &'a Path,
}

pub struct CommandBuffer {
    start: usize,
    size: usize,
}

impl From<serde_json::Error> for KvError {
    fn from(_: serde_json::Error) -> Self {
        KvError::SerializationError
    }
}

impl From<std::io::Error> for KvError {
    fn from(_: std::io::Error) -> Self {
        KvError::OpenError
    }
}

impl error::Error for KvError {}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvError::OpenError => write!(f, "Error opening log file"),
            KvError::RemoveError(ref key) => {
                write!(f, "Error: Cannot remove {} - the key does not exist", key)
            }
            KvError::NoLogPathError => write!(f, "Error: Log path not provided"),
            KvError::WriteError => write!(f, "Error writing to log file"),
            KvError::ReadLogError => write!(f, "Error reading the log file"),
            KvError::InvalidLogCommand => write!(f, "Error command in the log file"),
            KvError::SerializationError => write!(f, "Error serializing the information"),
        }
    }
}

impl<'a> KvStore<'a> {
    pub fn open(log_path: &Path) -> Result<KvStore> {
        let path = log_path.join("db.log");
        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(path.as_path())?;

        let mut store = KvStore {
            store: HashMap::new(),
            log_path: path,
            append_handle: file,
            log_size: 0,
            number_of_writes: 0,
            path: log_path,
        };

        ensure_file_exists(store.log_path.as_path())?;
        store.read_log_file()?;
        store.compact_log()?;
        Ok(store)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.increment_writes()?;

        let command = Command::Set {
            key: &key,
            value: &value,
        };

        let size = write_command_to_log_file(command, &mut self.append_handle)?;
        let command_buffer: CommandBuffer = CommandBuffer {
            start: self.log_size,
            size: size + 1,
        };
        self.log_size += size + 1;

        self.store.insert(key, command_buffer);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.increment_writes()?;

        if self.store.remove(&key).is_some() {
            let command = Command::Rm { key: &key };
            write_command_to_log_file(command, &mut self.append_handle)?;
            Ok(())
        } else {
            Err(KvError::RemoveError(key))
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<String>> {
        if let Some(value) = self.store.get(key) {
            let mut file = OpenOptions::new().read(true).open(&self.log_path)?;

            file.seek(SeekFrom::Start(value.start as u64))?;
            let mut buffer = vec![0; value.size as usize];
            file.read_exact(&mut buffer)?;

            let result = str::from_utf8(&buffer)?;
            let command: Command = serde_json::from_str(string)?;
            match command {
                Command::Set { key, value } => Ok(Some(value.to_string())),
                _ => Err(KvError::InvalidLogCommand),
            }
        } else {
            Ok(None)
        }
    }

    pub fn read_log_file(&mut self) -> Result<()> {
        let mut current_offset: usize = 0;
        let lines = read_lines(&self.log_path)?;

        for line_result in lines {
            let line = line_result?;
            self.read_line_into_store(&line, current_offset)?;
            current_offset += line.len() + 1;
        }

        self.log_size = current_offset;
        Ok(())
    }

    pub fn read_line_into_store(&mut self, line: &str, starting_offset: usize) -> Result<()> {
        let command: Command = serde_json::from_str(&line)?;
        let command_buffer: CommandBuffer = CommandBuffer {
            start: starting_offset,
            size: line.len(),
        };

        match command {
            Command::Rm { key } => {
                self.store.remove(&key.to_string());
                Ok(())
            }
            Command::Set { key, value } => {
                self.store.insert(key.to_string(), command_buffer);
                Ok(())
            }
            _ => Err(KvError::InvalidLogCommand),
        }
    }

    fn increment_writes(&mut self) -> Result<()> {
        self.number_of_writes += 1;

        if self.number_of_writes % 10_000 == 0 {
            self.compact_log()?;
        }

        Ok(())
    }

    fn compact_log(&mut self) -> Result<()> {
        let temp_log_file = self.path.join("temp.log");
        let log_file = self.path.join("db.log");

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_log_file)
            .unwrap();

        let mut updated_store: HashMap<String, CommandBuffer> = HashMap::new();
        let mut offset_start = 0;

        let mut number_of_keys = 0;
        self.store.iter().for_each(|(key, _)| {
            number_of_keys += 1;
            if let Ok(Some(value)) = self.get(key) {
                let command = Command::Set { key, value };
                let size = write_command_to_log_file(command, &mut file)?;
                let command_buffer = CommandBuffer {
                    start: offset_start,
                    size,
                };
                updated_store.insert(key.to_string(), command_buffer);
                offset_start += size + 1;
            }
        });

        if let Err(e) = fs::rename(temp_log_file, &log_file) {
            eprintln!("failed to rename log fie: {}", e);
        }

        self.store = updated_store;
        self.log_size = offset_start;
        self.append_handle = OpenOptions::new().append(true).open(&log_file)?;
        Ok(())
    }
}

fn write_command_to_log_file(command: Command, file_handle: &mut File) -> Result<usize> {
    let serialized = serde_json::to_string(&command)?;
    writeln!(file_handle, "{}", serialized)?;
    Ok(serialized.len())
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn ensure_file_exists<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    OpenOptions::new().create(true).write(true).open(path)?;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
enum Command<'a> {
    Set { key: &'a str, value: &'a str },
    Get { key: &'a str },
    Rm { key: &'a str },
}
