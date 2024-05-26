pub const ROOT_DIR: &str = "tests/workdir";

pub fn setup() {
    if std::fs::metadata(ROOT_DIR).is_err() {
        let _ = std::fs::create_dir(ROOT_DIR);
    }
}
