mod common;

use red::database::abstraction::{Column, DataType, Database, DatabaseTrait, Record, Table, DDL, DML};
use red::database::abstraction::RootDatabase;
use red::storage::files::{FileStorage, TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION};
use red::storage::persistence::DataHandler;

use crate::common::{setup, ROOT_DIR};

#[test]
fn test_database_abstraction_column_table() {
    let database = Database::new("customer", FileStorage::new(ROOT_DIR));
    let mut table = Table::new("users", Box::new(database));
    assert_eq!(table.get_name(), "users");
    assert_eq!(table.get_columns().len(), 0);

    let column_creation = Column::new("id", DataType::Integer, true, false);
    assert!(column_creation.is_ok());
    let column = column_creation.unwrap();
    table.add_column(column);
    assert_eq!(table.get_columns().len(), 1);
    assert_eq!(table.get_column_index("id").unwrap(), 0);
    
    let column_creation = Column::new("name", DataType::Text(255), false, false);
    assert!(column_creation.is_ok());
    let column = column_creation.unwrap();
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
    let column_creation = Column::new("id", DataType::Integer, true, false);
    assert!(column_creation.is_ok());
    let mut column = column_creation.unwrap();
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

    let mut root_database = RootDatabase::new(ROOT_DIR);
    let create_database = root_database.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let mut user_database = create_database.unwrap();

    // table creation with no columns
    let mut table = Table::new("users", Box::new(user_database.clone()));
    let result = user_database.create_table(table.clone());
    assert!(result.is_err());

    // table creation with columns
    let column_creation = Column::new("id", DataType::Integer, true, false);
    assert!(column_creation.is_ok());
    table.add_column(column_creation.unwrap());
    let column_creation = Column::new("name", DataType::Text(255), false, false);
    assert!(column_creation.is_ok());
    table.add_column(column_creation.unwrap());
    let result = user_database.create_table(table.clone());
    assert!(result.is_ok());

    let load_result = user_database.load_tables();
    assert!(load_result.is_ok());
    assert_eq!(user_database.get_tables().len(), 1);
    assert_eq!(user_database.get_tables()[0].get_name(), "users");

    let files = user_database.get_storage().list_files();
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

    let mut user_database = create_database.unwrap();

    let mut table = Table::new("users", Box::new(user_database.clone()));
    let column_creation = Column::new("id", DataType::Integer, true, false);
    assert!(column_creation.is_ok());
    table.add_column(column_creation.unwrap());
    let column_creation = Column::new("name", DataType::Text(255), false, false);
    assert!(column_creation.is_ok());
    table.add_column(column_creation.unwrap());
    let result = user_database.create_table(table.clone());
    assert!(result.is_ok());

    let result = user_database.create_table(table.clone());
    assert!(result.is_ok());

    let load_result = user_database.load_tables();
    assert!(load_result.is_ok());
    assert_eq!(user_database.get_tables().len(), 1);
    assert_eq!(user_database.get_tables()[0].get_name(), "users");

    let files = user_database.get_storage().list_files();
    let files = files.unwrap();
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DATA_EXTENSION))).to_owned());
    assert!(files.contains(&((table.get_name().to_owned() + "." + TABLE_FILE_DESCRIPTOR_EXTENSION))).to_owned());

    let result = user_database.drop_table(table.clone());
    assert!(result.is_ok());

    let load_result = user_database.load_tables();
    assert!(load_result.is_ok());
    assert!(user_database.get_tables().is_empty());

    let files = user_database.get_storage().list_files();
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

#[test]
fn test_insert_table_data() {
    setup();
    let root_path = "db_root_02";
    if std::fs::metadata(format!("{}/{}", ROOT_DIR, root_path)).is_ok() {
        std::fs::remove_dir_all(format!("{}/{}", ROOT_DIR, root_path)).unwrap();
    }
    std::fs::create_dir(format!("{}/{}", ROOT_DIR, root_path)).unwrap();

    let mut db_root = RootDatabase::new(&format!("{}/{}", ROOT_DIR, root_path));
    let db_name = "customer_01";
    let create_database = db_root.create_database(&db_name);
    assert!(create_database.is_ok());
    assert!(std::fs::metadata(format!("{}/{}", ROOT_DIR, db_name)).is_ok());

    let mut database = create_database.unwrap();
    let mut new_table = Table::new("users", Box::new(database.clone()));
    let column_creation = Column::new("id", DataType::Integer, true, false);
    assert!(column_creation.is_ok());
    let column = column_creation.unwrap();
    new_table.add_column(column);
    let column_creation = Column::new("name", DataType::Text(255), false, false);
    assert!(column_creation.is_ok());
    let column = column_creation.unwrap();
    new_table.add_column(column);

    let result = database.create_table(new_table.clone());
    let mut data_handler = DataHandler::new_from_path(format!("{}/{}/{}", ROOT_DIR, root_path, db_name));
    let columns_value = vec![
        (
            Column::new("id", DataType::Integer, true, false).unwrap(),
            Some("7".to_string())
        ), 
        (
            Column::new("name", DataType::Text(255), false, false).unwrap(), 
            Some("John Doe".to_string())
        )
    ];
    let new_record = Record::new(new_table.clone(), columns_value);
    let result = data_handler.insert(new_record);
    dbg!(&result);
    assert!(result.is_ok());
    assert!(result.unwrap() == 1);
    
}