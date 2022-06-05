/// The PDK interface for connectors
pub mod connectors;

use self::connectors::ConnectorMod_Ref;

use std::env;

use abi_stable::{
    declare_root_module_statics, library::RootModule, package_version_strings,
    sabi_types::VersionStrings, StableAbi,
};
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

/// This type defines the structure of an artefact plugin with the crate
/// `abi_stable`. It serves as a builder, making it possible to construct a
/// trait object of any kind of artefact.
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix))]
pub enum ArtefactMod {
    Connector(ConnectorPlugin),
}

/// `ArtefactMod_Ref` is the main module in this plugin declaration.
impl RootModule for ArtefactMod_Ref {
    /// The name of the dynamic library
    const BASE_NAME: &'static str = "artefact";
    /// The name of the library for logging and similars
    const NAME: &'static str = "artefact";
    /// The version of this plugin's crate
    const VERSION_STRINGS: VersionStrings = package_version_strings!();

    /// Implements the `RootModule::root_module_statics` function, which is the
    /// only required implementation for the `RootModule` trait.
    declare_root_module_statics! {ArtefactMod_Ref}
}

impl fmt::Debug for ArtefactMod_Ref {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("reference to artefact plugin")
    }
}

/// Note that `ArtefactMod_Ref` is just a pointer to the prefix of `Artefact`.
/// This is what we refer to as a 'plugin' most times, so we add this alias for
/// readabiity.
pub type ArtefactPlugin = ArtefactMod_Ref;
