use std::sync::Arc;

use super::TremorNodeId;
use super::TremorRaft;
use super::TremorStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct TremorApp {
    pub id: TremorNodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: TremorRaft,
    pub store: Arc<TremorStore>,
}

/*
impl TremorApp {

    pub(crate) fn new<P: AsRef<Path>>(
        id: TremorNodeId,
        api_addr: String,
        rpc_addr: String,
        raft: TremorRaft,
        store: Arc<TremorStore>,
    ) -> Self {
        Self {
            id,
            api_addr,
            rpc_addr,
            raft,
            store,
        }
    }
}
*/
