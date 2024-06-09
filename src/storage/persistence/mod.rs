// persistence is a module that contains the persistence logic for the storage module.

use crate::database::{self, abstraction::{DatabaseTrait, Table, DML}};
use crate::database::abstraction::Record;

use serde_json;

use super::files::{FileStorage, TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION};

pub struct DataHandler{
    storage: FileStorage
}

impl DataHandler {
    pub fn new_from_path(database_path: String) -> DataHandler {
        DataHandler{
            storage: FileStorage::new(&database_path)
        }
    }

    pub fn new_from_storage(storage: FileStorage) -> DataHandler {
        DataHandler{
            storage
        }
    }

    pub fn persist_table_descriptor(&self, table: &Table) -> Result<(), Box<dyn std::error::Error>>{
        // check if table has declared columns
        if table.get_columns().len() == 0 {
            return Err("Table has no columns".into());
        }
        // persist table descriptor
        let file_name = table.get_name().to_string()+"."+TABLE_FILE_DESCRIPTOR_EXTENSION;
        let content = serde_json::to_string_pretty(&table)?;
        self.storage.write_file(&file_name, &content)?;
        Ok(())
    }

    pub fn load_table_descriptor(&self, table_name: &str) -> Result<Table, Box<dyn std::error::Error>> {
        let file_name = table_name.to_string() + "." + TABLE_FILE_DESCRIPTOR_EXTENSION;
        let content = self.storage.read_file(&file_name)?;
        let table: Table = serde_json::from_str(&content)?;
        Ok(table)
    }

    pub fn persist_new_table(&self, table: &Table) -> Result<(), Box<dyn std::error::Error>>{
        // check if table has declared columns
        if table.get_columns().len() == 0 {
            return Err("Table has no columns".into());
        }
        // persist empty table data
        let file_name = table.get_name().to_string()+"."+TABLE_FILE_DATA_EXTENSION;
        let content = serde_json::to_string(&Vec::<Record>::new())?;
        // let database_path = table.get_database().get_root_dir();
        self.storage.write_file(&file_name, &content)?;
        Ok(())
    }    
}

impl DML for DataHandler {
    fn insert(&mut self, record: Record) -> Result<u32, Box<dyn std::error::Error>> {
        // check if record compability with table constraints
        let table_description = self.load_table_descriptor(record.get_table().get_name())?;
        if table_description.get_name() != record.get_table().get_name() {
            return Err("Table name mismatch".into());
        }
        if table_description.get_columns().len() != record.get_table().get_columns().len() {
            return Err("Column count mismatch".into());
        }
        if table_description.get_columns().len() != record.get_values().len() {
            return Err("Column count mismatch".into());
        }
        for (i, record_column) in record.get_values().iter().enumerate() {
            let table_column = &table_description.get_columns()[i];
            if record_column.0.get_name() != table_column.get_name() {
                return Err(format!("Column name mismatch for column {} {}", i, table_column.get_name()).into());
            }
            if record_column.0.get_data_type() != table_column.get_data_type() {
                return Err(format!("Column type mismatch for column {} {}", i, table_column.get_name()).into());
            }
            if !table_column.is_nullable() && record_column.1.is_none() {
                return Err(format!("Column value is null for not null column {} {}", i, table_column.get_name()).into());
            }
        }   

        // check if record column count matches table column count
        let record_columns = record.get_values();
        let table_columns = record.get_table().get_columns();
        if record_columns.len() != table_columns.len() {
            return Err("Column count mismatch".into());
        }
        // check if record column types matches table column types
        for (i, record_column) in record_columns.iter().enumerate() {
            let table_column = &table_columns[i];
            if record_column.0.get_data_type() != table_column.get_data_type() {
                return Err(format!("Column type mismatch for column {} {}", i, table_column.get_name()).into());
            }
        }
        // check if record column values are not null for not null columns
        for (i, record_column) in record_columns.iter().enumerate() {
            let table_column = &table_columns[i];
            if !table_column.is_nullable() && record_column.1.is_none() {
                return Err(format!("Column value is null for not null column {} {}", i, table_column.get_name()).into());
            }
        }
        // check if record column values are unique for unique columns
        // check if record column values are within the range for columns with range
        // check if record column values are within the length for columns with length
        // check if record column values are within the pattern for columns with pattern
        // check if record column values are within the enum for columns with enum

        // load table data
        let file_name = record.get_table().get_name().to_string() + "." + TABLE_FILE_DATA_EXTENSION;
        let content = self.storage.read_file(&file_name)?;
        let mut table_data: Vec<Record> = serde_json::from_str(&content)?;

        //check if record already exists
        for existing_record in table_data.iter() {
            if record == *existing_record {
                return Err("Record already exists".into());
            }
        }
        // add record to table data
        table_data.push(record.clone());

        // persist record
        let content = serde_json::to_string_pretty(&table_data)?;
        self.storage.write_file(&file_name, &content)?;

        Ok(1)
        
    }

    fn select(&self, query: crate::database::abstraction::Query ) -> Result<crate::database::abstraction::ResultSet, Box<dyn std::error::Error>> {
        todo!()
    }

    fn update(&mut self, record: crate::database::abstraction::Record, query: crate::database::abstraction::Query) -> Result<u32, Box<dyn std::error::Error>> {
        todo!()
    }

    fn delete(&mut self, query: crate::database::abstraction::Query) -> Result<u32, Box<dyn std::error::Error>> {
        todo!()
    }
}
