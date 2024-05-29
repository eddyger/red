// persistence is a module that contains the persistence logic for the storage module.

use crate::database::{self, abstraction::{DatabaseTrait, Table, DML}};
use serde_json;

use super::files::FileStorage;

pub struct DataHandler{
    storage: FileStorage
}

impl DataHandler {
    pub fn new(database_path: String) -> DataHandler {
        DataHandler{
            storage: FileStorage::new(&database_path)
        }
    }

    pub fn persist_table_descriptor(&self, table: &Table) -> Result<(), Box<dyn std::error::Error>>{
        // check if table has declared columns
        if table.get_columns().len() == 0 {
            return Err("Table has no columns".into());
        }
        // persist table descriptor
        let file_name = table.get_name();
        let content = serde_json::to_string_pretty(&table)?;
        // let database_path = table.get_database().get_root_dir();
        self.storage.write_file(file_name, &content)?;
        Ok(())
    }
}

impl DML for DataHandler {
    fn insert(&mut self, record: crate::database::abstraction::Record) -> Result<u32, Box<dyn std::error::Error>> {
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
        // check if record column values are unique for unique columns
        // check if record column values are within the range for columns with range
        // check if record column values are within the length for columns with length
        // check if record column values are within the pattern for columns with pattern
        // check if record column values are within the enum for columns with enum

        Ok(0)
        
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
