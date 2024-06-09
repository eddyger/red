// database abstraction layer

// table

use core::str;
use std::error::Error;

use crate::storage::{files::{FileExtension, FileStorage, TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION}, persistence::DataHandler};
use serde_derive::{Deserialize, Serialize};

pub trait DML {
    fn insert(&mut self, record: Record) -> Result<u32, Box<dyn Error>>;
    fn select(&self, query: Query ) -> Result<ResultSet, Box<dyn Error>>;
    fn update(&mut self, record: Record, query: Query) -> Result<u32, Box<dyn Error>>;
    fn delete(&mut self, query: Query) -> Result<u32, Box<dyn Error>>;
}

#[derive(Clone, Serialize, Deserialize,Debug)]
pub struct Table {
    database: Box<Database>,
    name: String,
    columns: Vec<Column>,
}

impl Default for Table {
    fn default() -> Self {
        Table {
            database: Box::new(Database::new("default", FileStorage::new("default"))),
            name: "default".to_string(),
            columns: Vec::new(),
        }
    }
}

impl Table {
    pub fn new(name: &str, database: Box<Database>) -> Table {
        Table {
            database,
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

    pub fn get_database(&self) -> &Database {
        &self.database
    }

    pub fn set_database(&mut self, database: Database) {
        self.database = Box::new(database);
    }


}

// column
#[derive(Clone,Serialize, Deserialize,Debug)]
pub struct Column {
    name: String,
    data_type: DataType,
    is_primary_key: bool,
    is_nullable: bool,
}

impl Column {
    pub fn new(name: &str, data_type: DataType, is_primary_key: bool, is_nullable: bool) -> Result<Column,String> {
        if is_primary_key && is_nullable {
            return Err("Primary key column cannot be nullable".to_string());
        }
        Ok(
            Column {
                name: name.to_string(),
                data_type,
                is_primary_key,
                is_nullable,
            }
        )
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

    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }

    pub fn is_nullable(&self) -> bool {
        self.is_nullable
    }

    pub fn set_primary_key(&mut self, is_primary_key: bool) {
        self.is_primary_key = is_primary_key;
    }

    pub fn set_nullable(&mut self, is_nullable: bool) {
        self.is_nullable = is_nullable;
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.data_type == other.data_type && self.is_primary_key == other.is_primary_key && self.is_nullable == other.is_nullable
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

#[derive(Debug, Clone,Serialize, Deserialize)]
pub struct Record {
    #[serde(skip)]
    table: Table,
    values: Vec<(Column,Option<String>)>,
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        if self.table.get_name() != other.table.get_name() {
            return false;
        }
        for (i, (column, value)) in self.values.iter().enumerate() {
            if column != &other.values[i].0 {
                return false;
            }
            if value != &other.values[i].1 {
                return false;
            }
        }
        return true;
    }
}

impl Record {
    pub fn new(table: Table, values: Vec<(Column,Option<String>)>) -> Record {
        Record {
            table,
            values,
        }
    }

    pub fn get_table(&self) -> &Table {
        &self.table
    }

    pub fn set_table(&mut self, table: Table) {
        self.table = table;
    }

    pub fn get_values(&self) -> &Vec<(Column,Option<String>)> {
        &self.values
    }

    pub fn set_values(&mut self, values: Vec<(Column,Option<String>)>) {
        self.values = values;
    }

}

pub struct ResultSet {
   records: Vec<Record>
}

impl ResultSet {
    pub fn new(records: Vec<Record>) -> ResultSet {
        ResultSet {
            records,
        }
    }

    pub fn default() -> ResultSet {
        ResultSet {
            records: Vec::new(),
        }
    }

    pub fn add_record(&mut self, record: Record) {
        self.records.push(record);
    }

    pub fn get_records(&self) -> &Vec<Record> {
        &self.records
    }

    pub fn set_records(&mut self, records: Vec<Record>) {
        self.records = records;
    }
}

pub struct Query {
    sql: String,
}

pub trait DDL {
    fn create_database(&mut self, name: &str) -> Result<Database, Box<dyn Error>>;
    fn drop_database(&mut self, name: &str) -> Result<(), Box<dyn Error>>;
    fn create_table(&mut self, table: Table) -> Result<(), Box<dyn Error>>;
    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn Error>>;
    fn alter_table(&mut self, table: Table, columns: Vec<Column>) -> Result<(), Box<dyn Error>>;
}

pub trait DatabaseTrait : DDL{
    fn load_tables(&mut self) -> Result<(), Box<dyn std::error::Error>>;
    fn get_name(&self) -> &str;
    fn get_storage(&self) -> &FileStorage;
    fn get_root_dir(&self) -> &str;
    fn get_tables(&self) -> &Vec<Table>;
}


pub struct RootDatabase {
    inner_database: Database,
    databases: Vec<Database>,
}

impl DatabaseTrait for RootDatabase{
    // load system tables
    fn load_tables(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Load tables from the database directory
        self.inner_database.load_tables()
    }
    
    fn get_name(&self) -> &str {
        self.inner_database.get_name()
    }
    
    fn get_storage(&self) -> &FileStorage {
        self.inner_database.get_storage()
    }
    
    fn get_root_dir(&self) -> &str {
        &self.inner_database.get_storage().get_root_dir()
    }
    
    fn get_tables(&self) -> &Vec<Table> {
        &self.inner_database.get_tables()
    }
}

impl RootDatabase {
    pub fn new(root_dir: &str) -> RootDatabase {
        RootDatabase {
            inner_database: Database::new("root_database", FileStorage::new(root_dir)),
            databases: Vec::new(),
        }
    }

    pub fn get_root_dir(&self) -> &str {
        &self.inner_database.get_storage().get_root_dir()
    }

    pub fn load_databases(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // (Re)Load databases from the root directory
        let databases = self.inner_database.get_storage().list_dirs()?;
        for database in databases {
            if self.databases.iter().any(|db| db.get_name() == database) {
                continue;
            }
            self.databases.push(Database::new(&database, FileStorage::new(&format!("{}/{}", self.inner_database.get_storage().get_root_dir(), database))));
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
        let new_database = self.inner_database.create_database(name)?;
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
        self.inner_database.create_table(table)?;
        Ok(())
    }

    // drop system table
    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file for table data and descriptor
        self.inner_database.drop_table(table)?;
        Ok(())
    }

    // alter system table
    fn alter_table(&mut self, _table: Table, _columns: Vec<Column>) -> Result<(), Box<dyn std::error::Error>> {
        todo!("Not implemented yet")
    }
}

#[derive(Clone,Deserialize, Serialize,Debug)]
pub struct Database {
    name: String,
    storage: FileStorage,
    tables: Vec<Table>,
}

impl DatabaseTrait for Database{
    fn load_tables(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Clear tables if not first call
        self.tables.clear();

        // Load tables from the database directory
        let tables = self.storage.list_files_with_extension(FileExtension::Both)?;
        let extension = ".".to_string() + TABLE_FILE_DESCRIPTOR_EXTENSION;
        for table in tables {
            if table.ends_with(extension.as_str()) {
                let table_name = table.trim_end_matches(extension.as_str());
                self.tables.push(Table::new(table_name,Box::new(self.clone())));
            }
        }
        Ok(())
    }
    
    fn get_name(&self) -> &str {
        &self.name
    }
    
    fn get_storage(&self) -> &FileStorage {
        &self.storage
    }
    
    fn get_root_dir(&self) -> &str {
        &self.storage.get_root_dir()
    }
    
    fn get_tables(&self) -> &Vec<Table> {
        &self.tables
    }
}  

impl Database {
    pub fn new(name: &str, storage: FileStorage) -> Database {
        Database {
            name: name.to_string(),
            storage,
            tables: Vec::new(),
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
        let data_handler = DataHandler::new_from_storage(self.storage.clone());
        data_handler.persist_table_descriptor(&table)?;
        data_handler.persist_new_table(&table)?;
        Ok(())
    }

    fn drop_table(&mut self, table: Table) -> Result<(), Box<dyn std::error::Error>> {
        // Delete a file for table data and descriptor
        self.storage.delete_file(&(table.get_name().to_string()+"."+TABLE_FILE_DATA_EXTENSION))?;
        self.storage.delete_file(&(table.get_name().to_string()+"."+TABLE_FILE_DESCRIPTOR_EXTENSION))?;
        Ok(())
    }

    fn alter_table(&mut self, table: Table, changed_columns: Vec<Column>) -> Result<(), Box<dyn std::error::Error>> {
        todo!("Not implemented yet")
    }
}
