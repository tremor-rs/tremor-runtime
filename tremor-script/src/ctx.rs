#[derive(Debug, Default, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize)]
pub struct EventContext {
    pub at: u64,
}

impl crate::registry::Context for EventContext {
    fn ingest_ns(&self) -> u64 {
        self.at
    }

    fn from_ingest_ns(ingest_ns: u64) -> Self {
        EventContext { at: ingest_ns }
    }
}
