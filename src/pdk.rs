/// The PDK interface for connectors
pub mod connectors;

use std::{env, fmt};

use abi_stable::library::RootModule;
use walkdir::WalkDir;

pub use self::connectors::{ConnectorPlugin, ConnectorPluginRef};

/// Default `TREMOR_PLUGIN_PATH`. Similarly to `TREMOR_PATH`, it contains the
/// following values by default:
///
/// * `/usr/share/tremor/plugins`: in packages this directory contains the
///   built-in plugins
/// * `/usr/local/share/tremor/plugins`: place for custom plugins
pub const DEFAULT_PLUGIN_PATH: &str = "/usr/local/share/tremor/plugins:/usr/share/tremor/plugins";

/// Recursively finds all the connector plugins in a directory. It doesn't
/// follow symlinks, and has a sensible maximum depth so that it doesn't get
/// stuck.
pub fn find_recursively(base_dir: &str) -> Vec<ConnectorPluginRef> {
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
        .filter_map(
            |file| match ConnectorPluginRef::load_from_file(file.path()) {
                Ok(plugin) => Some(plugin),
                Err(e) => {
                    log::debug!("Failed to load plugin in '{:?}': {}", file.path(), e);
                    None
                }
            },
        )
        .collect()
}
