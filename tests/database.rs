mod common;

use red::database::abstraction::{Column, DataType, DatabaseTrait, Table, DDL};
use red::database::abstraction::RootDatabase;
use red::storage::files::{TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION};

use crate::common::{setup, ROOT_DIR};

#[test]
fn test_database_abstraction_column_table() {
    let mut table = Table::new("users");
    assert_eq!(table.get_name(), "users");
    assert_eq!(table.get_columns().len(), 0);

    let column = Column::new("id", DataType::Integer);
    table.add_column(column);
    assert_eq!(table.get_columns().len(), 1);
    assert_eq!(table.get_column_index("id").unwrap(), 0);
    
    let column = Column::new("name", DataType::Text(255));
    table.add_column(column);
    assert_eq!(table.get_columns().len(), 2);
    assert_eq!(table.get_column_index("name").unwrap(), 1);
    
    let column = table.get_column("id").unwrap();
    assert_eq!(column.get_name(), "id");
    assert_eq!(*column.get_data_type(), DataType::Integer);
    
    let column = table.get_column("name").unwrap();
    assert_eq!(column.get_name(), "name");
    assert_eq!(*column.get_data_type(), DataType::Text(255));
    
    table.remove_column("id");
    assert_eq!(table.get_columns().len(), 1);
    
    table.remove_column("name");
    assert_eq!(table.get_columns().len(), 0);

    // change table name
    table.set_name("my_users");
    assert_eq!(table.get_name(), "my_users");
}

#[test]
fn test_database_abstraction_column() {
    let mut column = Column::new("id", DataType::Integer);
    assert_eq!(column.get_name(), "id");
    assert_eq!(*column.get_data_type(), DataType::Integer);

    // change data type
    column.set_data_type(DataType::Text(12));
    assert_eq!(*column.get_data_type(), DataType::Text(12));
    assert_eq!(12, match column.get_data_type() {
        DataType::Text(size) => *size,
        _ => 0
    });

    // change name
    column.set_name("name");
    assert_eq!(column.get_name(), "name");
}

#[test]
fn test_create_table() {
    setup();
    let db_name = "customer";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, db_name)).unwrap();
    }

    let mut db_root = RootDatabase::new(ROOT_DIR);
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let mut database = create_database.unwrap();

    let table = Table::new("users");
    let result = database.create_table(table.clone());
    assert!(result.is_ok());

    let tables = database.load_tables();
    assert!(tables.is_ok());
    let tables = tables.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].get_name(), "users");

    let files = database.get_storage().list_files();
    let files = files.unwrap();
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DATA_EXTENSION))).to_owned());
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DESCRIPTOR_EXTENSION))).to_owned());
}

#[test]
fn test_drop_table() {
    setup();
    let db_name = "customer_01";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, db_name)).unwrap();
    }

    let mut db_root = RootDatabase::new(ROOT_DIR);
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let mut database = create_database.unwrap();

    let table = Table::new("users");
    let result = database.create_table(table.clone());
    assert!(result.is_ok());

    let tables = database.load_tables();
    assert!(tables.is_ok());
    let tables = tables.unwrap();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].get_name(), "users");

    let files = database.get_storage().list_files();
    let files = files.unwrap();
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DATA_EXTENSION))).to_owned());
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DESCRIPTOR_EXTENSION))).to_owned());

    let result = database.drop_table(table.clone());
    assert!(result.is_ok());

    let tables = database.load_tables();
    assert!(tables.is_ok());
    let tables = tables.unwrap();
    assert!(tables.is_empty());

    let files = database.get_storage().list_files();
    let files = files.unwrap();
    assert!(!files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DATA_EXTENSION))).to_owned());
    assert!(!files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DESCRIPTOR_EXTENSION))).to_owned());
}

#[test]
fn test_create_database() {
    setup();
    let db_name = "customer_02";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, db_name)).unwrap();
    }

    let mut db_root = RootDatabase::new(ROOT_DIR);
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());
}

#[test]
fn test_drop_database() {
    setup();
    let db_name = "customer_03";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, db_name)).unwrap();
    }

    let mut db_root = RootDatabase::new(ROOT_DIR);
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let result = db_root.drop_database(&db_name);
    assert!(result.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_err());
}

#[test]
fn test_load_databases() {
    setup();
    let root_path = "db_root";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, root_path)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, root_path)).unwrap();
    }
    std::fs::create_dir(format!("{}/{}", ROOT_DIR, root_path)).unwrap();

    let mut db_root = RootDatabase::new(&format!("{}/{}", ROOT_DIR, root_path));
    let db_name = "customer_04";
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let result = db_root.load_databases();
    assert!(result.is_ok());
    let databases = db_root.get_databases();
    assert_eq!(databases.len(), 1);
    assert_eq!(databases[0].get_name(), db_name);
}