mod common;
use red::storage::{self, files::{FileExtension,TABLE_FILE_DATA_EXTENSION, TABLE_FILE_DESCRIPTOR_EXTENSION}};

use crate::common::{setup, ROOT_DIR};

#[test]
fn test_create_dir() {
    setup();
    let dir_name = "test_create_dir";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_dir(&dir_name);
    assert!(result.is_ok());

    let result = storage.create_dir(&dir_name);
    assert!(result.is_err());
}

#[test]
fn test_delete_dir() {
    setup();
    let dir_name = "test_delete_dir";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_dir(&dir_name);
    assert!(result.is_ok());

    let result = storage.delete_dir(&dir_name);
    assert!(result.is_ok());

    let result = storage.delete_dir(&dir_name);
    assert!(result.is_err());
}

#[test]
fn test_create_file() {
    setup();
    let file_name = "test_create_file";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_file(&file_name);
    assert!(result.is_ok());

    let result = storage.create_file(&file_name);
    assert!(result.is_err());
}

#[test]
fn test_delete_file() {
    setup();
    let file_name = "test_delete_file";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_file(&file_name);
    assert!(result.is_ok());

    let result = storage.delete_file(&file_name);
    assert!(result.is_ok());

    let result = storage.delete_file(&file_name);
    assert!(result.is_err());
}

#[test]
fn test_read_write_file() {
    setup();
    let file_name = "test_read_file";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    
    let content = "Hello, World!";
    let result = storage.write_file(&file_name, content);
    assert!(result.is_ok());

    let result = storage.read_file(&file_name);
    assert!(result.is_ok());
    assert_eq!(content, result.unwrap());
}

#[test]
fn test_read_file_not_exists() {
    setup();
    let file_name = "test_read_file_not_exists";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    
    let result = storage.read_file(&file_name);
    assert!(result.is_err());
}

#[test]
fn test_write_file_exists() {
    setup();
    let file_name = "test_write_file_exists";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    
    let content = "Hello, World!";
    let result = storage.write_file(&file_name, content);
    assert!(result.is_ok());

    let content = "Hello, World! 2";
    let result = storage.write_file(&file_name, content);
    assert!(result.is_err());
}

#[test]
fn test_append_file() {
    setup();
    let file_name = "test_append_file";
    let test_file = format!("{}/{}", ROOT_DIR, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    
    let content = "Hello, World!";
    let result = storage.write_file(&file_name, content);
    assert!(result.is_ok());

    let content = " 2";
    let result = storage.append_file(&file_name, content);
    assert!(result.is_ok());

    let result = storage.read_file(&file_name);
    assert!(result.is_ok());
    assert_eq!("Hello, World! 2", result.unwrap());
}

#[test]
fn test_list_files() {
    setup();

    let dir_name = "test_liste_dir";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_dir(&dir_name);
    assert!(result.is_ok());

    let file_name = "test_list_files";
    let test_file = format!("{}/{}", test_dir, file_name);
    if std::fs::metadata(&test_file).is_ok() {
        std::fs::remove_file(&test_file).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(&test_dir);
    
    let content = "Hello, World!";
    let result = storage.write_file(&file_name, content);
    assert!(result.is_ok());

    let files = storage.list_files();
    assert!(files.is_ok());
    assert!(files.unwrap().contains(&file_name.to_owned()));
}

#[test]
fn test_list_files_empty() {
    setup();

    let dir_name = "test_list_files_empty";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_dir(&dir_name);
    assert!(result.is_ok());

    let storage = storage::files::FileStorage::new(&test_dir);
    let files = storage.list_files();
    assert!(files.is_ok());
    assert!(files.unwrap().is_empty());
}

#[test]
fn test_list_files_not_exists() {
    setup();

    let dir_name = "test_list_files_not_exists";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(&test_dir);
    let files = storage.list_files();
    assert!(files.is_err());
}

#[test]
fn test_list_files_with_extension() {
    setup();

    let dir_name = "test_list_files_with_extension";
    let test_dir = format!("{}/{}", ROOT_DIR, dir_name);
    if std::fs::metadata(&test_dir).is_ok() {
        std::fs::remove_dir_all(&test_dir).unwrap();
    }
    
    let storage = storage::files::FileStorage::new(ROOT_DIR);
    let result = storage.create_dir(&dir_name);
    assert!(result.is_ok());

    // data file
    let data_file_name = "test_list_files_with_extension.".to_string() + TABLE_FILE_DATA_EXTENSION;
    let storage = storage::files::FileStorage::new(&test_dir);
    let content = "Hello, Data!";
    let result = storage.write_file(&data_file_name, content);
    assert!(result.is_ok());

    // descriptor file
    let descriptor_file_name = "test_list_files_with_extension.".to_string() + TABLE_FILE_DESCRIPTOR_EXTENSION;
    let storage = storage::files::FileStorage::new(&test_dir);
    let content = "Hello, Descriptor!";
    let result = storage.write_file(&descriptor_file_name, content);
    assert!(result.is_ok());


    let files = storage.list_files_with_extension(FileExtension::Data);
    assert!(files.is_ok());
    let files = files.unwrap();
    assert!(files.contains(&data_file_name.to_owned()));
    assert!(!files.contains(&descriptor_file_name.to_owned()));

    let files = storage.list_files_with_extension(FileExtension::Descriptor);
    assert!(files.is_ok());
    let files = files.unwrap();
    assert!(!files.contains(&data_file_name.to_owned()));
    assert!(files.contains(&descriptor_file_name.to_owned()));

    let files = storage.list_files_with_extension(FileExtension::Both);
    assert!(files.is_ok());
    let files = files.unwrap();
    assert!(files.contains(&data_file_name.to_owned()));
    assert!(files.contains(&descriptor_file_name.to_owned()));

}