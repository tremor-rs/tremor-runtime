uname -a
ls -a
make chk_copyright_ci
make chk_unwrap_ci
make chk_panic_ci
source $HOME/.cargo/env
export CARGO_HOME=.cargo
cargo test --all-features
cargo test -p tremor-script -p window
cargo clippy --all-features
cp target/debug/native/php-src/libs/* /lib64
make it
