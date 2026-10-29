use std::env;
use std::path::PathBuf;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let header_dir =
        env::var("ONELAKE_READER_HEADER_DIR").unwrap_or_else(|_| env::var("OUT_DIR").unwrap());
    let header_path = PathBuf::from(header_dir).join("onelake_table_reader.h");
    std::fs::create_dir_all(header_path.parent().unwrap())
        .expect("failed to create reader header directory");
    cbindgen::generate(&crate_dir)
        .expect("Unable to generate reader bindings")
        .write_to_file(header_path);
    println!("cargo:rerun-if-env-changed=ONELAKE_READER_HEADER_DIR");
    println!("cargo:rerun-if-changed=src/ffi.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
