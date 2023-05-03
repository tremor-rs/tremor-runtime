// Copyright 2021, The Tremor Team
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

use postgres_protocol::message::backend::{
    Column, LogicalReplicationMessage, ReplicaIdentity, Tuple, TupleData, XLogDataBody,
};
use serde::ser::{Error, SerializeStruct};
use serde::{Serialize, Serializer};
use std::fmt;

/// `SerializedTuple`: A wrapper struct around a Tuple object that provides a Serialize implementation
/// for it. This struct is used to serialize the tuple data of a `PostgreSQL` message into JSON format.

pub(crate) struct SerializedTuple<'a>(pub &'a Tuple);

impl<'a> Serialize for SerializedTuple<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let data = self
            .0
            .tuple_data()
            .iter()
            .map(|d| SerializedTupleData::from_tuple_data(d))
            .collect::<Vec<_>>();
        let mut state = serializer.serialize_struct("Tuple", 1)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

/// `SerializedOptionTuple`: Similar to `SerializedTuple`, but for an optional Tuple object.
/// If the inner Option is Some, this struct serializes the tuple data. If the inner Option is None,
/// this struct serializes None.
pub(crate) struct SerializedOptionTuple<'a>(pub Option<&'a Tuple>);

impl<'a> Serialize for SerializedOptionTuple<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Some(tuple) => {
                let data = tuple
                    .tuple_data()
                    .iter()
                    .map(SerializedTupleData::from_tuple_data)
                    .collect::<Vec<_>>();
                let mut state = serializer.serialize_struct("Tuple", 1)?;
                state.serialize_field("data", &data)?;
                state.end()
            }
            None => serializer.serialize_none(),
        }
    }
}

impl<'a, 'b> From<&'b Tuple> for SerializedTuple<'a>
where
    'b: 'a,
{
    fn from(tuple: &'b Tuple) -> Self {
        SerializedTuple(tuple)
    }
}

/// `SerializedTupleData` : An enum that represents the different types of data that can be present in
/// a tuple. This struct is used by `SerializedTuple` and `SerializedOptionTuple`
/// to serialize tuple data into JSON format.
#[derive(Serialize)]
#[serde(untagged)]
enum SerializedTupleData {
    Null,
    UnchangedToast,
    Text(String),
}

impl SerializedTupleData {
    fn from_tuple_data(data: &TupleData) -> SerializedTupleData {
        match data {
            TupleData::Null => SerializedTupleData::Null,
            TupleData::UnchangedToast => SerializedTupleData::UnchangedToast,
            TupleData::Text(bytes) => {
                SerializedTupleData::Text(String::from_utf8_lossy(bytes.as_ref()).to_string())
            }
        }
    }
}

/// `SerializedReplicaIdentity` : An enum that represents the different types of replica identity that
/// can be configured for a table in `PostgreSQL`. This struct is used to serialize replica identity
/// information into JSON format.
#[derive(Serialize)]
enum SerializedReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}
impl SerializedReplicaIdentity {
    fn from_replica_identity(replica_identity: &ReplicaIdentity) -> SerializedReplicaIdentity {
        match replica_identity {
            ReplicaIdentity::Default => SerializedReplicaIdentity::Default,
            ReplicaIdentity::Nothing => SerializedReplicaIdentity::Nothing,
            ReplicaIdentity::Full => SerializedReplicaIdentity::Full,
            ReplicaIdentity::Index => SerializedReplicaIdentity::Index,
        }
    }
}

/// `SerializedColumn` : A struct that represents a `PostgreSQL` column, with information about
/// the column's flags, name, type ID, and type modifier.
/// This struct is used to serialize column information into JSON format.
#[derive(Debug, Serialize)]
pub struct SerializedColumn<'a> {
    flags: i8,
    name: &'a str,
    type_id: i32,
    type_modifier: i32,
}

impl<'a> From<&'a Column> for SerializedColumn<'a> {
    fn from(column: &'a Column) -> Self {
        SerializedColumn {
            flags: column.flags(),
            name: column.name().unwrap(), // this will panic if there's an error reading the name
            type_id: column.type_id(),
            type_modifier: column.type_modifier(),
        }
    }
}

/// `SerializedColumns` : A wrapper struct around an array of Column objects that provides a
/// From implementation for it. This struct is used to serialize
/// an array of columns into JSON format.
#[derive(Debug, Serialize)]
pub struct SerializedColumns<'a> {
    columns: Vec<SerializedColumn<'a>>,
}

impl<'a> From<&'a [Column]> for SerializedColumns<'a> {
    fn from(columns: &'a [Column]) -> Self {
        SerializedColumns {
            columns: columns.iter().map(SerializedColumn::from).collect(),
        }
    }
}

/// `CustomError`: A struct that represents a custom error type.
/// This struct is used to wrap errors from the `std::io` module and
/// provide a custom error message when they occur.
#[derive(Debug)]
pub struct CustomError {
    message: String,
}

impl From<std::io::Error> for CustomError {
    fn from(error: std::io::Error) -> Self {
        CustomError {
            message: error.to_string(),
        }
    }
}

impl Serialize for CustomError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.message)
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
/// `SerializedXLogDataBody`: A wrapper struct around a `XLogDataBody` object that provides a
/// Serialize implementation for it.
/// This struct is used to serialize logical replication messages into JSON format.
#[derive(Debug)]
pub(crate) struct SerializedXLogDataBody<T>(pub XLogDataBody<T>);

impl Serialize for SerializedXLogDataBody<LogicalReplicationMessage> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let data = &SerializedLogicalReplicationMessage(self.0.data());
        let mut state = serializer.serialize_struct("XLogDataBody", 3)?;
        state.serialize_field("wal_start", &self.0.wal_start())?;
        state.serialize_field("wal_end", &self.0.wal_end())?;
        state.serialize_field("timestamp", &self.0.timestamp())?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

/// `SerializedLogicalReplicationMessage` : A wrapper struct around a `LogicalReplicationMessage` object
/// that provides a Serialize implementation for it.
/// This struct is used by `SerializedXLogDataBody` to serialize
/// the data field of a logical replication message into JSON format.
pub(crate) struct SerializedLogicalReplicationMessage<'a>(pub &'a LogicalReplicationMessage);

impl<'a> Serialize for SerializedLogicalReplicationMessage<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LogicalReplicationMessage", 5)?;
        match self.0 {
            LogicalReplicationMessage::Begin(ref msg) => {
                state.serialize_field("final_lsn", &msg.final_lsn())?;
                state.serialize_field("timestamp", &msg.timestamp())?;
                state.serialize_field("xid", &msg.xid())?;
            }
            LogicalReplicationMessage::Commit(ref msg) => {
                state.serialize_field("flags", &msg.flags())?;
                state.serialize_field("commit_lsn", &msg.commit_lsn())?;
                state.serialize_field("end_lsn", &msg.end_lsn())?;
                state.serialize_field("timestamp", &msg.timestamp())?;
            }
            LogicalReplicationMessage::Origin(ref msg) => {
                state.serialize_field("commit_lsn", &msg.commit_lsn())?;
                let name = msg
                    .name()
                    .map_err(CustomError::from)
                    .map_err(S::Error::custom)?;
                state.serialize_field("name", &name)?;
            }
            LogicalReplicationMessage::Relation(ref msg) => {
                state.serialize_field("type", "RELATION")?;
                state.serialize_field("rel_id", &msg.rel_id())?;
                let namespace = msg
                    .namespace()
                    .map_err(CustomError::from)
                    .map_err(S::Error::custom)?;
                let name = msg
                    .name()
                    .map_err(CustomError::from)
                    .map_err(S::Error::custom)?;
                state.serialize_field("namespace", &namespace)?;
                state.serialize_field("name", &name)?;
                state.serialize_field(
                    "replica_identity",
                    &SerializedReplicaIdentity::from_replica_identity(msg.replica_identity()),
                )?;
                state.serialize_field("columns", &SerializedColumns::from(msg.columns()))?;
            }
            LogicalReplicationMessage::Type(ref msg) => {
                state.serialize_field("id", &msg.id())?;
                let namespace = msg
                    .namespace()
                    .map_err(CustomError::from)
                    .map_err(S::Error::custom)?;
                let name = msg
                    .name()
                    .map_err(CustomError::from)
                    .map_err(S::Error::custom)?;
                state.serialize_field("namespace", &namespace)?;
                state.serialize_field("name", &name)?;
            }
            LogicalReplicationMessage::Insert(ref msg) => {
                state.serialize_field("type", "INSERT")?;
                state.serialize_field("rel_id", &msg.rel_id())?;
                let serialized_tuple = SerializedTuple(msg.tuple());
                state.serialize_field("tuple", &serialized_tuple)?;
            }
            LogicalReplicationMessage::Update(ref msg) => {
                state.serialize_field("type", "UPDATE")?;
                state.serialize_field("rel_id", &msg.rel_id())?;
                if let Some(_old_tuple) = &msg.old_tuple() {
                    let old_tuple = SerializedOptionTuple(msg.old_tuple());
                    state.serialize_field("old_tuple", &old_tuple)?;
                }
                if let Some(_key_tuple) = &msg.key_tuple() {
                    let key_tuple = SerializedOptionTuple(msg.key_tuple());
                    state.serialize_field("key_tuple", &key_tuple)?;
                }
                let new_tuple = SerializedTuple(msg.new_tuple());
                state.serialize_field("new_tuple", &new_tuple)?;
            }
            LogicalReplicationMessage::Delete(ref msg) => {
                state.serialize_field("type", "DELETE")?;
                state.serialize_field("rel_id", &msg.rel_id())?;
                if let Some(_old_tuple) = &msg.old_tuple() {
                    let old_tuple = SerializedOptionTuple(msg.old_tuple());
                    state.serialize_field("old_tuple", &old_tuple)?;
                }
                if let Some(_key_tuple) = &msg.key_tuple() {
                    let key_tuple = SerializedOptionTuple(msg.key_tuple());
                    state.serialize_field("key_tuple", &key_tuple)?;
                }
            }
            LogicalReplicationMessage::Truncate(ref msg) => {
                state.serialize_field("options", &msg.options())?;
                state.serialize_field("rel_ids", &msg.rel_ids())?;
            }
            _ => {}
        }
        state.end()
    }
}
