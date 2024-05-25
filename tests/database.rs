// tests database abstraction Column Table

use red::database::abstraction::{Table,Column,DataType};

#[test]
fn test_database_abstraction_column_table() {
    let mut table = Table::new("users");
    assert_eq!(table.get_name(), "users");
    assert_eq!(table.get_columns().len(), 0);

    let column = Column::new("id", DataType::Integer);
    table.add_column(column);
    assert_eq!(table.get_columns().len(), 1);
    assert_eq!(table.get_column_index("id").unwrap(), 0);
    
    let column = Column::new("name", DataType::Text);
    table.add_column(column);
    assert_eq!(table.get_columns().len(), 2);
    assert_eq!(table.get_column_index("name").unwrap(), 1);
    
    let column = table.get_column("id").unwrap();
    assert_eq!(column.get_name(), "id");
    assert_eq!(*column.get_data_type(), DataType::Integer);
    
    let column = table.get_column("name").unwrap();
    assert_eq!(column.get_name(), "name");
    assert_eq!(*column.get_data_type(), DataType::Text);
    
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
    column.set_data_type(DataType::Text);
    assert_eq!(*column.get_data_type(), DataType::Text);

    // change name
    column.set_name("name");
    assert_eq!(column.get_name(), "name");
}