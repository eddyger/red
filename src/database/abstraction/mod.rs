// database abstraction layer

// table

use core::str;
use std::error::Error;

use crate::storage::files::{FileExtension, FileStorage, TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION};
use serde_derive::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new(name: &str) -> Table {
        Table {
            name: name.to_string(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn set_columns(&mut self, columns: Vec<Column>) {
        self.columns = columns;
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|&column| column.name == name)
    }

    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        for (index, column) in self.columns.iter().enumerate() {
            if column.name == name {
                return Some(index);
            }
        }
        None
    }

    pub fn remove_column(&mut self, name: &str) {
        if let Some(index) = self.get_column_index(name) {
            self.columns.remove(index);
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
}

// column
#[derive(Clone,Serialize, Deserialize)]
pub struct Column {
    name: String,
    data_type: DataType,
}

impl Column {
    pub fn new(name: &str, data_type: DataType) -> Column {
        Column {
            name: name.to_string(),
            data_type,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn set_data_type(&mut self, data_type: DataType) {
        self.data_type = data_type;
    }

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_string();
    }
}

// data type
#[derive(Debug, PartialEq, Clone,Serialize, Deserialize)]
pub enum DataType {
    Text(u16),
    Integer,
    Real,
    Blob,
}

pub trait DatabaseTrait : DDL{
    fn load_tables(&self) -> Result<Vec<Table>, Box<dyn std::error::Error>>;
}


pub struct RootDatabase {
    root_dir: String,
    inner_database: Database,
    databases: Vec<Database>,
}

impl DatabaseTrait for RootDatabase{
    // load system tables
    fn load_tables(&self) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
        // Load tables from the database directory
        self.inner_database.load_tables()
    }
}

impl RootDatabase {
    pub fn new(root_dir: &str) -> RootDatabase {
        RootDatabase {
            root_dir: root_dir.to_string(),
            inner_database: Database::new("root_database", FileStorage::new(root_dir)),
            databases: Vec::new(),
        }
    }

    pub fn get_root_dir(&self) -> &str {
        &self.root_dir
    }

    pub fn load_databases(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // (Re)Load databases from the root directory
        let databases = self.inner_database.get_storage().list_dirs()?;
        for database in databases {
            if self.databases.iter().any(|db| db.get_name() == database) {
                continue;
            }
            self.databases.push(Database::new(&database, FileStorage::new(&format!("{}/{}", self.root_dir, database))));
        }
        Ok(())
    }

    pub fn get_databases(&self) -> &Vec<Database> {
        &self.databases
    }

    pub fn get_database(&self, name: &str) -> Option<&Database> {
        self.databases.iter().find(|&database| database.get_name() == name)
    }

}

impl DDL for RootDatabase {
    fn create_database(&mut self, name: &str) -> Result<Database, Box<dyn std::error::Error>> {
        self.inner_database.get_storage().create_dir(name)?;
        let new_database = Database::new(name, FileStorage::new(&format!("{}/{}", self.root_dir, name)));
        self.databases.push(new_database.clone());
        Ok(new_database)
    }

    fn drop_database(&mut self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.inner_database.get_storage().delete_dir(name)?;
        self.databases.retain(|database| database.get_name() != name);
        Ok(())
    }
    
    // create system table
    fn create_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Create a file for table data and descriptor
        self.inner_database.get_storage().create_file(&(table.get_name().to_string()+TABLE_FILE_DATA_EXTENSION))?;
        self.inner_database.get_storage().create_file(&(table.get_name().to_string()+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    // drop system table
    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file for table data and descriptor
        self.inner_database.get_storage().delete_file(&(table.get_name().to_string()+TABLE_FILE_DATA_EXTENSION))?;
        self.inner_database.get_storage().delete_file(&(table.get_name().to_string()+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    // alter system table
    fn alter_table(&mut self, _table: Table, _columns: Vec<Column>) -> Result<(), Box<dyn std::error::Error>> {
        todo!("Not implemented yet")
    }
}

pub trait DDL {
    fn create_database(&mut self, name: &str) -> Result<Database, Box<dyn Error>>;
    fn drop_database(&mut self, name: &str) -> Result<(), Box<dyn Error>>;
    fn create_table(&mut self, table: Table) -> Result<(), Box<dyn Error>>;
    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn Error>>;
    fn alter_table(&mut self, table: Table, columns: Vec<Column>) -> Result<(), Box<dyn Error>>;
}

#[derive(Clone)]
pub struct Database {
    name: String,
    storage: FileStorage,
}

impl DatabaseTrait for Database{
    fn load_tables(&self) -> Result<Vec<Table>, Box<dyn std::error::Error>> {
        // Load tables from the database directory
        let tables = self.storage.list_files_with_extension(FileExtension::Both)?;
        let mut result = Vec::new();
        let extension = ".".to_string() + TABLE_FILE_DESCRIPTOR_EXTENSION;
        for table in tables {
            if table.ends_with(extension.as_str()) {
                let table_name = table.trim_end_matches(extension.as_str());
                result.push(Table::new(table_name));
            }
        }
        Ok(result)
    }
}  

impl Database {
    pub fn new(name: &str, storage: FileStorage) -> Database {
        Database {
            name: name.to_string(),
            storage,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_storage(&self) -> &FileStorage {
        &self.storage
    }

}

impl DDL for Database {
    // create a schema
    fn create_database(&mut self, name: &str) -> Result<Database, Box<dyn std::error::Error>> {
        self.storage.create_dir(name)?;
        Ok(Database::new(name, FileStorage::new(&format!("{}/{}", self.storage.get_root_dir(), name))))
    }

    // drop a schema
    fn drop_database(&mut self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.storage.delete_dir(name)?;
        Ok(())
    }
    
    fn create_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Create a file for table data and descriptor
        self.storage.create_file(&(table.get_name().to_string()+"."+TABLE_FILE_DATA_EXTENSION))?;
        self.storage.create_file(&(table.get_name().to_string()+"."+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file for table data and descriptor
        self.storage.delete_file(&(table.get_name().to_string()+"."+TABLE_FILE_DATA_EXTENSION))?;
        self.storage.delete_file(&(table.get_name().to_string()+"."+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    fn alter_table(&mut self, _table: Table, _columns: Vec<Column>) -> Result<(), Box<dyn std::error::Error>> {
        todo!("Not implemented yet")
    }
}
