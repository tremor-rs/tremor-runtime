/// The PDK interface for connectors
pub mod connectors;

use self::connectors::ConnectorMod_Ref;

use std::env;

use abi_stable::library::RootModule;
use walkdir::WalkDir;

/// The error type used for the PDK, from `abi_stable`
pub type RError = abi_stable::std_types::SendRBoxError;
/// The result type used for the PDK, from `abi_stable`
pub type RResult<T> = abi_stable::std_types::RResult<T, RError>;

/// Default `TREMOR_PLUGIN_PATH`. Similarly to `TREMOR_PATH`, it contains the
/// following values by default:
///
/// * `/usr/share/tremor/plugins`: in packages this directory contains the
///   built-in plugins
/// * `/usr/local/share/tremor/plugins`: place for custom plugins
pub const DEFAULT_PLUGIN_PATH: &str = "/usr/local/share/tremor/plugins:/usr/share/tremor/plugins";

/// This can be used alongside [`abi_stable::rtry`] for error handling. Its
/// difference is that it does an implicit conversion to `RResult`, which is
/// useful sometimes to reduce boilerplate.
///
/// These macros are a workaround until `?` can be used with functions that
/// return `RResult`: https://github.com/rust-lang/rust/issues/84277.
#[macro_export]
macro_rules! ttry {
    ($e:expr) => {
        match $e {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => {
                return ::abi_stable::std_types::RResult::RErr(err.into())
            }
        }
    };
}

/// Recursively finds all the connector plugins in a directory. It doesn't
/// follow symlinks, and has a sensible maximum depth so that it doesn't get
/// stuck.
pub fn find_recursively(base_dir: &str) -> Vec<ConnectorMod_Ref> {
    WalkDir::new(base_dir)
        // No symlinks are followed for now
        .follow_links(false)
        // Adding some safe limits
        .max_depth(1000)
        .into_iter()
        // Ignoring permission errors
        .filter_map(Result::ok)
        // Only try to load those that look like plugins on the current platform
        .filter(|file| {
            file.path()
                .extension()
                .map(|ext| ext == env::consts::DLL_EXTENSION)
                .unwrap_or(false)
        })
        // Try to load the plugins and if successful, add them to the result.
        // Not being able to load a plugin shouldn't be fatal because it's very
        // likely in some situations. Errors will just be printed to the logs.
        .filter_map(|file| match ConnectorMod_Ref::load_from_file(file.path()) {
            Ok(plugin) => Some(plugin),
            Err(e) => {
                log::debug!("Failed to load plugin in '{:?}': {}", file.path(), e);
                None
            }
        })
        .collect()
}

pub mod utils {
    use abi_stable::std_types::RCow;

    // FIXME: clean up after creation of `tremor-pdk`, this is repeated in other
    // crates.
    pub fn conv_cow_str(cow: RCow<str>) -> beef::Cow<str> {
        let cow: std::borrow::Cow<str> = cow.into();
        cow.into()
    }

    pub fn conv_cow_str_inv(cow: beef::Cow<str>) -> RCow<str> {
        let cow: std::borrow::Cow<str> = cow.into();
        cow.into()
    }
}
