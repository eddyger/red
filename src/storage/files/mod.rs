use std::{collections::HashMap, io::Write};
use crate::database::{abstraction::{Column, Table}, ddl::DDL};

// Storage for files database

// Texte file storage
pub struct FileStorage {
    root_dir: String,
}

// File extension for table data and descriptor
pub const TABLE_FILE_DATA_EXTENSION : &str = "data";
pub const TABLE_FILE_DESCRIPTOR_EXTENSION: &str  = "desc";

#[derive(Debug, Eq, PartialEq, Hash)]
pub enum FileExtension {
    Data,
    Descriptor,
    Both
}

pub fn get_file_type_and_extension() -> HashMap<FileExtension, String>  
{
    let mut map = HashMap::new();
    map.insert(FileExtension::Data, TABLE_FILE_DATA_EXTENSION.to_owned());
    map.insert(FileExtension::Descriptor, TABLE_FILE_DESCRIPTOR_EXTENSION.to_owned());
    map
}

impl FileStorage {
    pub fn new(root_path: &str) -> FileStorage {
        FileStorage {
            root_dir: root_path.to_string(),
        }
    }

    pub fn get_root_dir(&self) -> &str {
        &self.root_dir
    }

    pub fn create_dir(&self, dir_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Create a directory if not exists yet
        let path = format!("{}/{}", self.root_dir, dir_name);
        std::fs::create_dir(path)?;
        Ok(())
    }

    pub fn delete_dir(&self, dir_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a directory if exists
        let path = format!("{}/{}", self.root_dir, dir_name);
        std::fs::remove_dir(path)?;
        Ok(())
    }

    pub fn create_file(&self, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Create a file if not exists yet
        let path = format!("{}/{}", self.root_dir, file_name);
        if std::fs::metadata(path.clone()).is_ok() {
            return Err("File already exists".into());
        }
        std::fs::File::create(path)?;
        Ok(())
    }

    pub fn delete_file(&self, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file if exists
        let path = format!("{}/{}", self.root_dir, file_name);
        std::fs::remove_file(path)?;
        Ok(())
    }

    pub fn read_file(&self, file_name: &str) -> Result<String, Box<dyn std::error::Error>> {
        // Read a file content
        let path = format!("{}/{}", self.root_dir, file_name);
        let content = std::fs::read_to_string(path)?;
        Ok(content)
    }

    pub fn write_file(&self, file_name: &str, content: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Write a file content if file does not exist. if file exists, it will throw an error
        let path = format!("{}/{}", self.root_dir, file_name);
        if std::fs::metadata(path.clone()).is_ok() {
            return Err("File already exists".into());
        }
        std::fs::write(path, content)?;
        Ok(())
    }

    pub fn append_file(&self, file_name: &str, content: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Append a file content if file exist. if file does not exists, it will throw an error
        let path = format!("{}/{}", self.root_dir, file_name);
        if std::fs::metadata(path.clone()).is_err() {
            return Err("File does not exist".into());
        }
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    // List all files in the root directory
    pub fn list_files(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        return self.list_files_with_extension(FileExtension::Both);
    }

    pub fn list_files_with_extension(&self, extension: FileExtension) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // List files with specific extension in the root directory
        let mut files = Vec::new();
        let extensions = get_file_type_and_extension();
        let _extension = if extensions.get(&extension).is_some() { 
            extensions.get(&extension).unwrap() 
        } else {
             "" 
        };

        for entry in std::fs::read_dir(&self.root_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && (extension == FileExtension::Both || path.extension().unwrap().to_str().unwrap() == _extension) {
                files.push(entry.file_name().into_string().unwrap());
            }
        }
        Ok(files)
    }
}

impl DDL for FileStorage {
    fn create_database(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.create_dir(name)?;
        Ok(())
    }

    fn drop_database(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.delete_dir(name)?;
        Ok(())
    }
    
    fn create_table(&self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Create a file for table data and descriptor
        self.create_file(&(table.get_name().to_string()+TABLE_FILE_DATA_EXTENSION))?;
        self.create_file(&(table.get_name().to_string()+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    fn drop_table(&self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file for table data and descriptor
        self.delete_file(&(table.get_name().to_string()+TABLE_FILE_DATA_EXTENSION))?;
        self.delete_file(&(table.get_name().to_string()+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    fn alter_table(&self, _table: Table, _columns: Vec<Column>) -> Result<(), Box<dyn std::error::Error>> {
        todo!("Not implemented yet")
    }
}
