// persistence is a module that contains the persistence logic for the storage module.

use crate::database::abstraction::Table;
use serde_json;

use super::files::FileStorage;

pub struct FileTableDescriptor {
    table: Table,
    file_name: String
}

impl FileTableDescriptor {
    pub fn new(table: Table, file_name: String) -> FileTableDescriptor {
        FileTableDescriptor {
            table,
            file_name
        }
    }

    pub fn get_table(&self) -> &Table {
        &self.table
    }

    pub fn set_table(&mut self, table: Table) {
        self.table = table;
    }
}

pub struct FileTableData {
    table: Table,
    file_name: String
}

impl FileTableData {
    pub fn new(table: Table, file_name: String) -> FileTableData {
        FileTableData {
            table,
            file_name
        }
    }

    pub fn get_table(&self) -> &Table {
        &self.table
    }

    pub fn set_table(&mut self, table: Table) {
        self.table = table;
    }
}

pub struct DataPersister{
    storage: FileStorage
}

impl DataPersister {
    pub fn new(database_path: String) -> DataPersister {
        DataPersister{
            storage: FileStorage::new(&database_path)
        }
    }

    pub fn persist_table_descriptor(&self, table_descriptor: FileTableDescriptor) -> Result<(), Box<dyn std::error::Error>>{
        // persist table descriptor
        let table = table_descriptor.get_table();
        let file_name = table_descriptor.file_name.clone(); // Clone the file_name field
        let content = serde_json::to_string(table)?;
        self.storage.write_file(&file_name, &content)?;
        Ok(())
    }
}
