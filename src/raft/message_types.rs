// Copyright 2018-2020, Wayfair GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use raft::eraftpb;
use serde_derive::{Deserialize, Serialize};

pub type NodeId = u64;

pub trait Model {}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum EventType {
    Hup,
    Beat,
    Propose,
    Append,
    AppendResponse,
    RequestVote,
    RequestVoteResponse,
    Snapshot,
    Heartbeat,
    HeartbeatResponse,
    Unreachable,
    SnapStatus,
    CheckQuorum,
    TransferLeader,
    TimeoutNow,
    ReadIndex,
    ReadIndexResp,
    RequestPreVote,
    RequestPreVoteResponse,
}
impl Model for EventType {}

impl From<eraftpb::MessageType> for EventType {
    fn from(other: eraftpb::MessageType) -> EventType {
        use eraftpb::MessageType;
        match other {
            MessageType::MsgHup => EventType::Hup,
            MessageType::MsgBeat => EventType::Beat,
            MessageType::MsgPropose => EventType::Propose,
            MessageType::MsgAppend => EventType::Append,
            MessageType::MsgAppendResponse => EventType::AppendResponse,
            MessageType::MsgRequestVote => EventType::RequestVote,
            MessageType::MsgRequestVoteResponse => EventType::RequestVoteResponse,
            MessageType::MsgSnapshot => EventType::Snapshot,
            MessageType::MsgHeartbeat => EventType::Heartbeat,
            MessageType::MsgHeartbeatResponse => EventType::HeartbeatResponse,
            MessageType::MsgUnreachable => EventType::Unreachable,
            MessageType::MsgSnapStatus => EventType::SnapStatus,
            MessageType::MsgCheckQuorum => EventType::CheckQuorum,
            MessageType::MsgTransferLeader => EventType::TransferLeader,
            MessageType::MsgTimeoutNow => EventType::TimeoutNow,
            MessageType::MsgReadIndex => EventType::ReadIndex,
            MessageType::MsgReadIndexResp => EventType::ReadIndexResp,
            MessageType::MsgRequestPreVote => EventType::RequestPreVote,
            MessageType::MsgRequestPreVoteResponse => EventType::RequestPreVoteResponse,
        }
    }
}

impl Into<eraftpb::MessageType> for EventType {
    fn into(self) -> eraftpb::MessageType {
        use eraftpb::MessageType;
        match self {
            EventType::Hup => MessageType::MsgHup,
            EventType::Beat => MessageType::MsgBeat,
            EventType::Propose => MessageType::MsgPropose,
            EventType::Append => MessageType::MsgAppend,
            EventType::AppendResponse => MessageType::MsgAppendResponse,
            EventType::RequestVote => MessageType::MsgRequestVote,
            EventType::RequestVoteResponse => MessageType::MsgRequestVoteResponse,
            EventType::Snapshot => MessageType::MsgSnapshot,
            EventType::Heartbeat => MessageType::MsgHeartbeat,
            EventType::HeartbeatResponse => MessageType::MsgHeartbeatResponse,
            EventType::Unreachable => MessageType::MsgUnreachable,
            EventType::SnapStatus => MessageType::MsgSnapStatus,
            EventType::CheckQuorum => MessageType::MsgCheckQuorum,
            EventType::TransferLeader => MessageType::MsgTransferLeader,
            EventType::TimeoutNow => MessageType::MsgTimeoutNow,
            EventType::ReadIndex => MessageType::MsgReadIndex,
            EventType::ReadIndexResp => MessageType::MsgReadIndexResp,
            EventType::RequestPreVote => MessageType::MsgRequestPreVote,
            EventType::RequestPreVoteResponse => MessageType::MsgRequestPreVoteResponse,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum EntryType {
    Normal,
    ConfChange,
    ConfChangeV2,
}
impl Model for EntryType {}

impl From<eraftpb::EntryType> for EntryType {
    fn from(other: eraftpb::EntryType) -> EntryType {
        match other {
            eraftpb::EntryType::EntryNormal => EntryType::Normal,
            eraftpb::EntryType::EntryConfChange => EntryType::ConfChange,
            eraftpb::EntryType::EntryConfChangeV2 => EntryType::ConfChangeV2,
        }
    }
}

impl Into<eraftpb::EntryType> for EntryType {
    fn into(self) -> eraftpb::EntryType {
        use eraftpb::EntryType;
        match self {
            self::EntryType::Normal => EntryType::EntryNormal,
            self::EntryType::ConfChange => EntryType::EntryConfChange,
            self::EntryType::ConfChangeV2 => EntryType::EntryConfChangeV2,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Entry {
    kind: EntryType,
    term: u64,
    index: u64,
    data: Vec<u8>,
    context: Vec<u8>,
}
impl Model for Entry {}

impl From<&eraftpb::Entry> for Entry {
    fn from(other: &eraftpb::Entry) -> Entry {
        Entry {
            kind: other.entry_type.into(),
            term: other.term,
            index: other.index,
            data: other.data.clone(),
            context: other.context.clone(),
        }
    }
}

impl Into<eraftpb::Entry> for &Entry {
    fn into(self) -> eraftpb::Entry {
        let mut entry = eraftpb::Entry::default();
        entry.entry_type = self.kind.clone().into();
        entry.term = self.term;
        entry.index = self.index;
        entry.data = self.data.clone();
        entry.context = self.context.clone();
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub struct Snapshot {
    data: Vec<u8>,
    meta: Option<SnapshotMetadata>,
}
impl Model for Snapshot {}

impl From<eraftpb::Snapshot> for Snapshot {
    fn from(other: eraftpb::Snapshot) -> Snapshot {
        Snapshot {
            data: other.data,
            meta: match other.metadata.into_option() {
                Some(v) => Some(v.into()),
                None => None,
            },
        }
    }
}

impl Into<eraftpb::Snapshot> for Snapshot {
    fn into(self) -> eraftpb::Snapshot {
        use protobuf::SingularPtrField;
        let mut entry = eraftpb::Snapshot::default();
        entry.data = self.data;
        entry.metadata = match self.meta {
            Some(value) => SingularPtrField::some(value.into()),
            None => SingularPtrField::none(),
        };
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub struct SnapshotMetadata {
    term: u64,
    index: u64,
    state: Option<State>,
}
impl Model for SnapshotMetadata {}

impl From<eraftpb::SnapshotMetadata> for SnapshotMetadata {
    fn from(other: eraftpb::SnapshotMetadata) -> SnapshotMetadata {
        SnapshotMetadata {
            term: other.term,
            index: other.index,
            state: match other.conf_state.into_option() {
                Some(value) => Some(value.into()),
                None => None,
            },
        }
    }
}

impl Into<eraftpb::SnapshotMetadata> for SnapshotMetadata {
    fn into(self) -> eraftpb::SnapshotMetadata {
        use protobuf::SingularPtrField;
        let mut entry = eraftpb::SnapshotMetadata::default();
        entry.term = self.term;
        entry.index = self.index;
        entry.conf_state = match self.state {
            Some(value) => SingularPtrField::some(value.into()),
            None => SingularPtrField::none(),
        };
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Event {
    pub(crate) kind: EventType,
    term: u64,
    index: u64,
    to: NodeId,
    from: NodeId,
    log_term: u64,
    commit: u64,
    snapshot: Option<Snapshot>,
    request_snapshot: u64,
    reject: bool,
    reject_hint: u64,
    context: Vec<u8>,
    entries: Vec<Entry>,
}
impl Model for Event {}

impl From<eraftpb::Message> for Event {
    fn from(other: eraftpb::Message) -> Event {
        Event {
            kind: other.msg_type.into(),
            term: other.term,
            index: other.index,
            to: other.to.into(),
            from: other.from.into(),
            log_term: other.log_term,
            commit: other.commit,
            snapshot: match other.snapshot.into_option() {
                None => None,
                Some(v) => Some(v.into()),
            },
            request_snapshot: other.request_snapshot,
            reject: other.reject,
            reject_hint: other.reject_hint,
            context: other.context,
            entries: other.entries.iter().map(|e| e.into()).collect(),
        }
    }
}

impl Into<eraftpb::Message> for Event {
    fn into(self) -> eraftpb::Message {
        use protobuf::{RepeatedField, SingularPtrField};
        let mut entry = eraftpb::Message::default();
        entry.msg_type = self.kind.into();
        entry.term = self.term;
        entry.index = self.index;
        entry.to = self.to.into();
        entry.from = self.from.into();
        entry.log_term = self.log_term;
        entry.commit = self.commit;
        entry.snapshot = match self.snapshot {
            Some(v) => SingularPtrField::some(v.into()),
            None => SingularPtrField::none(),
        };
        entry.request_snapshot = self.request_snapshot;
        entry.reject = self.reject;
        entry.reject_hint = self.reject_hint;
        entry.context = self.context;
        entry.entries = RepeatedField::<eraftpb::Entry>::from_vec(
            self.entries.iter().map(|e| e.into()).collect(),
        );
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub struct HardState {
    term: u64,
    vote: u64,
    commit: u64,
}
impl Model for HardState {}

impl From<eraftpb::HardState> for HardState {
    fn from(other: eraftpb::HardState) -> HardState {
        HardState {
            term: other.term,
            vote: other.vote,
            commit: other.commit,
        }
    }
}

impl Into<eraftpb::HardState> for HardState {
    fn into(self) -> eraftpb::HardState {
        let mut entry = eraftpb::HardState::default();
        entry.term = self.term;
        entry.vote = self.vote;
        entry.commit = self.commit;
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug, Default)]
pub struct State {
    voters: Vec<u64>,
    learners: Vec<u64>,
    voters_outgoing: Vec<u64>,
    learners_next: Vec<u64>,
    auto_leave: bool,
}
impl Model for State {}

impl From<eraftpb::ConfState> for State {
    fn from(other: eraftpb::ConfState) -> State {
        State {
            voters: other.voters,
            learners: other.learners,
            voters_outgoing: other.voters_outgoing,
            learners_next: other.learners_next,
            auto_leave: other.auto_leave,
        }
    }
}

impl Into<eraftpb::ConfState> for State {
    fn into(self) -> eraftpb::ConfState {
        let mut entry = eraftpb::ConfState::default();
        entry.voters = self.voters;
        entry.learners = self.learners;
        entry.voters_outgoing = self.voters_outgoing;
        entry.learners_next = self.learners_next;
        entry.auto_leave = self.auto_leave;
        entry
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum StateChangeType {
    AddNode,
    RemoveNode,
    AddLearnerNode,
}
impl Model for StateChangeType {}

impl From<eraftpb::ConfChangeType> for StateChangeType {
    fn from(other: eraftpb::ConfChangeType) -> StateChangeType {
        use eraftpb::ConfChangeType;
        match other {
            ConfChangeType::AddNode => StateChangeType::AddNode,
            ConfChangeType::RemoveNode => StateChangeType::RemoveNode,
            ConfChangeType::AddLearnerNode => StateChangeType::AddLearnerNode,
        }
    }
}

impl Into<eraftpb::ConfChangeType> for StateChangeType {
    fn into(self) -> eraftpb::ConfChangeType {
        use eraftpb::ConfChangeType;
        match self {
            StateChangeType::AddNode => ConfChangeType::AddNode,
            StateChangeType::RemoveNode => ConfChangeType::RemoveNode,
            StateChangeType::AddLearnerNode => ConfChangeType::AddLearnerNode,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct StateChange {
    kind: StateChangeType,
    node_id: u64,
    id: u64,
    context: Vec<u8>,
}
impl Model for StateChange {}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct StateChangeSingle {
    kind: StateChangeType,
    node_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum StateChangeTransition {
    Auto,
    Implicit,
    Explicit,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct StateChangeV2 {
    transition: StateChangeTransition,
    changes: Vec<StateChangeSingle>,
    context: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default)]
    struct TestError {}
    impl std::fmt::Display for TestError {
        fn fmt(&self, _fmt: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
            // Discard
            Ok(())
        }
    }
    impl std::error::Error for TestError {}

    macro_rules! assert_symmetric {
        ($t:ident, $c:expr) => {
            let ss = serde_json::to_string($c);
            if let Ok(s) = ss {
                {
                    let mirror_image: $t = serde_json::from_str(&s).expect("fixme");
                    assert_eq!(*$c, mirror_image);
                }
                let _ = s.to_owned();
            }
        };
    }

    #[test]
    fn test_entry_pb_json_conversion() -> Result<(), TestError> {
        let mut pb = eraftpb::Entry::default();
        pb.term = 1;
        pb.index = 2;
        pb.data = vec![3u8];
        pb.context = vec![4u8];

        let json: Entry = (&pb).into();
        assert_eq!(json.term, 1);
        assert_eq!(json.index, 2);
        assert_eq!(json.data[0], 3);
        assert_eq!(json.context[0], 4);

        let pb2: eraftpb::Entry = (&json).into();

        assert_eq!(pb2, pb);

        Ok(())
    }

    #[test]
    fn test_raft_model_serde_symmetric_and_defaults() -> Result<(), TestError> {
        let candidate = HardState::default();

        assert_eq!(0, candidate.term);
        assert_eq!(0, candidate.vote);
        assert_eq!(0, candidate.commit);

        assert_symmetric!(HardState, &candidate);

        println!(
            "{}",
            serde_json::to_string_pretty(&candidate).expect("not ok")
        );

        Ok(())
    }
}
