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
    RemoveError,
    ReadLogError,
    InvalidLogCommand,
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
        KvError::WriteError
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
            KvError::RemoveError => write!(f, "Error: Cannot remove key that does not exist"),
            KvError::NoLogPathError => write!(f, "Error: Log path not provided"),
            KvError::WriteError => write!(f, "Error writing to log file"),
            KvError::ReadLogError => write!(f, "Error reading the log file"),
            KvError::InvalidLogCommand => write!(f, "Error command in the log file"),
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
            key: key.clone(),
            value: value.clone(),
        };
        let mut handle = self.append_handle.try_clone()?;
        let size = write_command_to_log_file(command, &mut handle)?;
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
        let value = self.store.remove(&key);

        if value.is_some() {
            let command = Command::Rm { key };
            let mut handle = self.append_handle.try_clone()?;
            write_command_to_log_file(command, &mut handle).unwrap();
            Ok(())
        } else {
            Err(KvError::RemoveError)
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.store.get(&key);
        if let Some(value) = value {
            let mut file = OpenOptions::new()
                .read(true)
                .open(self.log_path.as_path())?;

            file.seek(SeekFrom::Start(value.start as u64))?;
            let mut buffer = vec![0; value.size as usize];
            file.read_exact(&mut buffer)?;

            let result = str::from_utf8(&buffer);

            match result {
                Ok(string) => {
                    println!("{}", string);
                    let command: Command = serde_json::from_str(string)?;
                    match command {
                        Command::Set { key, value } => Ok(Some(value)),
                        _ => Err(KvError::InvalidLogCommand),
                    }
                }
                Err(_) => Err(KvError::InvalidLogCommand),
            }
        } else {
            Ok(None)
        }
    }

    pub fn read_log_file(&mut self) -> Result<()> {
        let mut current_offset: usize = 0;

        match read_lines(self.log_path.as_path()) {
            Ok(lines) => {
                for line_result in lines {
                    match line_result {
                        Ok(line) => {
                            self.read_line_into_store(line.clone(), current_offset)?;
                            current_offset += line.len() + 1;
                        }
                        Err(e) => {
                            eprintln!("error reading line: {}", e);
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("error reading file: {}", e);
            }
        }

        self.log_size = current_offset;
        Ok(())
    }

    pub fn read_line_into_store(&mut self, line: String, starting_offset: usize) -> Result<()> {
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
            println!("Compacting the log");
            self.compact_log().unwrap();
        }

        Ok(())
    }

    fn compact_log(&mut self) -> Result<()> {
        let temp_log_file = self.path.join("temp.log");
        let log_file = self.path.join("db.log");

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(temp_log_file.clone())
            .unwrap();

        let mut updated_store: HashMap<String, CommandBuffer> = HashMap::new();
        let mut offset_start = 0;

        let mut number_of_keys = 0;
        for (key, _) in self.store.iter() {
            let result = self.get(key.clone());
            number_of_keys += 1;
            match result {
                Ok(value) => {
                    if let Some(value) = value {
                        let command = Command::Set {
                            key: key.to_string(),
                            value,
                        };
                        let size = write_command_to_log_file(command, &mut file).unwrap();
                        let command_buffer = CommandBuffer {
                            start: offset_start,
                            size,
                        };
                        updated_store.insert(key.to_string(), command_buffer);
                        offset_start += size + 1;
                    }
                }
                Err(e) => (),
            }
        }

        println!("Number of Keys: {}", number_of_keys);
        match fs::rename(temp_log_file, log_file.clone()) {
            Ok(_) => (),
            Err(e) => {
                eprintln!("failed to rename log fie: {}", e);
            }
        }

        self.store = updated_store;
        self.log_size = offset_start;
        self.append_handle = OpenOptions::new()
            .append(true)
            .open(log_file.clone())
            .unwrap();
        Ok(())
    }
}
fn write_command_to_log_file(command: Command, file_handle: &mut File) -> Result<usize> {
    let serialized = serde_json::to_string(&command);
    match serialized {
        Ok(serialized) => {
            let result = writeln!(file_handle, "{}", serialized);
            match result {
                Ok(_) => Ok(serialized.len()),
                Err(e) => {
                    eprintln!("Error serializing command: {}", e);
                    Err(KvError::WriteError)
                }
            }
        }
        Err(e) => {
            eprintln!("Error serializing command: {}", e);
            Err(KvError::WriteError)
        }
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = OpenOptions::new().read(true).open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn ensure_file_exists<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        let _ = File::create(path)?;
    }
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}
