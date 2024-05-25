// database abstraction layer

// table

use core::str;

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
        for column in &self.columns {
            if column.name == name {
                return Some(column);
            }
        }
        None
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
#[derive(Debug, PartialEq)]
pub enum DataType {
    Text,
    Integer,
    Real,
    Blob,
}