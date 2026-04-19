fn main() {
    #[cfg(feature = "ffi")]
    {
        let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        let out_dir = std::path::PathBuf::from(&crate_dir).join("include");
        std::fs::create_dir_all(&out_dir).unwrap();
        let out_path = out_dir.join("gcflobdd.h");

        cbindgen::Builder::new()
            .with_crate(&crate_dir)
            .with_config(cbindgen::Config::from_file(format!("{}/cbindgen.toml", crate_dir)).unwrap())
            .generate()
            .expect("Failed to generate C bindings")
            .write_to_file(&out_path);

        println!("cargo:rerun-if-changed=src/ffi.rs");
        println!("cargo:rerun-if-changed=cbindgen.toml");
    }
}
