use std::{collections::HashMap, io::Write};

// Storage for files database

// Texte file storage
#[derive(Clone)]
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
        self.list_files_with_extension(FileExtension::Both)
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

    //List all directories in the root directory
    pub fn list_dirs(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        // List directories in the root directory
        let mut dirs = Vec::new();
        for entry in std::fs::read_dir(&self.root_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                dirs.push(entry.file_name().into_string().unwrap());
            }
        }
        Ok(dirs)
    }
}
