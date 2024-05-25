use std::error::Error;

use crate::database::abstraction::{Column,Table}; 

// Data definition language (DDL) module

pub trait DDL {
    fn create_database(&self, name: &str) -> Result<(), Box<dyn Error>>;
    fn drop_database(&self, name: &str) -> Result<(), Box<dyn Error>>;
    fn create_table(&self, table: Table) -> Result<(), Box<dyn Error>>;
    fn drop_table(&self, table: Table) -> Result<(), Box<dyn Error>>;
    fn alter_table(&self, table: Table, columns: Vec<Column>) -> Result<(), Box<dyn Error>>;
}