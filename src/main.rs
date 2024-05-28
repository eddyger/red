use red::{database::abstraction::{Database, Table}, storage::files::FileStorage};


fn main() {
    let table = Table::new("users", Box::new(Database::new("customer", FileStorage::new("root_dir"))));
    println!("Hello, world!");
}