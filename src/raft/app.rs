use std::sync::Arc;

use super::TremorNodeId;
use super::TremorRaft;
use super::TremorStore;

// Representation of an application state. This struct can be shared around to share
// instances of raft, store and more.
pub struct TremorApp {
    pub id: TremorNodeId,
    pub api_addr: String,
    pub rcp_addr: String,
    pub raft: TremorRaft,
    pub store: Arc<TremorStore>,
}
