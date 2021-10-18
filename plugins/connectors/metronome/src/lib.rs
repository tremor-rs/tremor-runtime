use abi_stable::{
    export_root_module,
    type_level::downcasting::TD_Opaque,
    prefix_type::PrefixTypeTrait,
    rstr, sabi_extern_fn,
    std_types::{RBox, RStr, RResult::ROk},
};

use tremor_runtime::connectors::{
    reconnect, Connector, ConnectorContext, ConnectorMod, ConnectorMod_Ref, ConnectorState,
    Connector_TO,
    RResult,
};

struct Metronome;

impl Connector for Metronome {
    /* async */
    fn connect(
        &mut self,
        _ctx: &ConnectorContext,
        _notifier: reconnect::ConnectionLostNotifier,
    ) -> RResult<bool> {
        ROk(true)
    }

    /* async */
    fn on_start(&mut self, _ctx: &ConnectorContext) -> RResult<ConnectorState> {
        ROk(0)
    }

    fn default_codec(&self) -> RStr<'_> {
        rstr!("application/json")
    }
}

/// Exports the root module of this library.
///
/// This code isn't run until the layout of the type it returns is checked.
#[export_root_module]
fn instantiate_root_module() -> ConnectorMod_Ref {
    ConnectorMod { new }.leak_into_prefix()
}

#[sabi_extern_fn]
pub fn new() -> Connector_TO<'static, RBox<()>> {
    let metronome = Metronome;
    // We don't need to be able to downcast the connector back to the original
    // type, so we just pass it as an opaque type.
    Connector_TO::from_value(metronome, TD_Opaque)
}
