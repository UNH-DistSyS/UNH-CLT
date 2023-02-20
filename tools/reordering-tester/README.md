# Reordering Tester

This is a simple application designed to be used to test packet reordering under various conditions.
It is written in Rust because calling into liburing from Go incurs too much overhead for some testing 
scenarios. Arguments are shared between both binaries (reordering-tester-server and 
reordering-tester-client), and both command lines should be identical. 

Please note that workers without rate limitations are capable of saturating links, and packets may be 
dropped. Any data analysis should assume that dropped packets are due to this unless you can show that
the total transmission rate should be well below line rate. Smaller packets are more prone to this issue
as they fill up kernel-side buffers faster. 

## Building 

### Requirements

* liburing-dev
* A modern version of clang
* Rust 1.67.1+

### Command

```bash
cargo build --release
```

This will put the binaries into target/release/

## Running 

### Requirements

* Linux 5.15+ (Newer is better)

The executables are mostly statically linked but do dynamically link against libc and libm. If you want to fully statically link, use
your platform's musl target.