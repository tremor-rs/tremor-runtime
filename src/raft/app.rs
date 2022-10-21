use crate::raft;
use std::sync::Arc;

/// Representation of an application state. This struct can be shared around to share
/// instances of raft, store and more.
pub struct Tremor {
    pub id: raft::NodeId,
    pub api_addr: String,
    pub rpc_addr: String,
    pub raft: raft::Tremor,
    pub store: Arc<raft::Store>,
}
