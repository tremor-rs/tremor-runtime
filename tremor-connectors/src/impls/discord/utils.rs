// Copyright 2020-2021, The Tremor Team
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

// #![cfg_attr(coverage, no_coverage)] // We need a life discord api for this
use serenity::{
    model::{
        channel::{GuildChannel, Message, Reaction, ReactionType},
        event::{
            GuildMemberUpdateEvent, GuildMembersChunkEvent, ResumedEvent, TypingStartEvent,
            VoiceServerUpdateEvent,
        },
        guild::{Emoji, Guild, Member, PartialGuild, Role},
        id::{ChannelId, EmojiId, GuildId, MessageId, RoleId},
        prelude::{CurrentUser, Presence, User, VoiceState},
    },
    prelude::*,
};
use std::collections::HashMap;
use tremor_value::prelude::*;

#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
pub(crate) enum Intents {
    /// Guilds Grouping
    Guilds,
    /// GuildMembers Grouping
    GuildMembers,
    /// GuildModeration Grouping
    GuildModeration,
    /// GuildEmojis Grouping
    GuildEmojis,
    /// GuildIntegrations Grouping
    GuildIntegrations,
    /// GuildWebHooks Grouping
    GuildWebHooks,
    /// GuildInvites Grouping
    GuildInvites,
    /// GuildVoiceStates
    GuildVoiceStates,
    /// GuildPresence
    GuildPresence,
    /// GuildMessages
    GuildMessages,
    /// GuildMessageReactions
    GuildMessageReactions,
    /// GuildMessageTyping
    GuildMessageTyping,
    /// DirectMessages
    DirectMessages,
    /// DirectMessageReactions
    DirectMessageReactions,
    /// DirectMessageTyping
    DirectMessageTyping,
    /// MessageContent
    MessageContent,
    /// ScheduledEvents
    ScheduledEvents,
    /// All intents
    All,
    /// All non privileged details
    NonPrivileged,
}

impl From<Intents> for GatewayIntents {
    fn from(intent: Intents) -> GatewayIntents {
        match intent {
            Intents::Guilds => GatewayIntents::GUILDS,
            Intents::GuildMembers => GatewayIntents::GUILD_MEMBERS,
            Intents::GuildModeration => GatewayIntents::GUILD_MODERATION,
            Intents::GuildEmojis => GatewayIntents::GUILD_EMOJIS_AND_STICKERS,
            Intents::GuildIntegrations => GatewayIntents::GUILD_INTEGRATIONS,
            Intents::GuildWebHooks => GatewayIntents::GUILD_WEBHOOKS,
            Intents::GuildInvites => GatewayIntents::GUILD_INVITES,
            Intents::GuildVoiceStates => GatewayIntents::GUILD_VOICE_STATES,
            Intents::GuildPresence => GatewayIntents::GUILD_PRESENCES,
            Intents::GuildMessages => GatewayIntents::GUILD_MESSAGES,
            Intents::GuildMessageReactions => GatewayIntents::GUILD_MESSAGE_REACTIONS,
            Intents::GuildMessageTyping => GatewayIntents::GUILD_MESSAGE_TYPING,
            Intents::DirectMessages => GatewayIntents::DIRECT_MESSAGES,
            Intents::DirectMessageReactions => GatewayIntents::DIRECT_MESSAGE_REACTIONS,
            Intents::DirectMessageTyping => GatewayIntents::DIRECT_MESSAGE_TYPING,
            Intents::MessageContent => GatewayIntents::MESSAGE_CONTENT,
            Intents::ScheduledEvents => GatewayIntents::GUILD_SCHEDULED_EVENTS,
            Intents::All => GatewayIntents::all(),
            Intents::NonPrivileged => GatewayIntents::non_privileged(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize)]
pub(crate) enum DiscordMessage {
    AddReaction(Reaction),
    RemoveReaction(Reaction),
    ChannelCreate(GuildChannel),
    ChannelDelete(GuildChannel, Option<Vec<Message>>),
    ChannelUpdate {
        old: Option<GuildChannel>,
        new: GuildChannel,
    },
    BanAddition {
        guild_id: GuildId,
        user: User,
    },
    BanRemoval {
        guild_id: GuildId,
        user: User,
    },
    GuildCreate {
        guild: Guild,
        is_new: Option<bool>,
    },
    // GuildDelete {
    //     incomplete: GuildUnavailable,
    //     full: Option<Guild>,
    // },
    EmojiUpdate {
        guild_id: GuildId,
        current_state: HashMap<EmojiId, Emoji>,
    },
    IntegrationsUpdate(GuildId),
    MemberAddition {
        new_member: Member,
    },
    MemberRemoval {
        guild_id: GuildId,
        user: User,
        member_data_if_available: Option<Member>,
    },
    MemberUpdate {
        old_if_available: Option<Member>,
        member: Option<Member>,
        event: GuildMemberUpdateEvent,
    },
    RoleCreate {
        new: Role,
    },
    RoleDelete {
        guild_id: GuildId,
        removed_role_id: RoleId,
        removed_role_data_if_available: Option<Role>,
    },
    RoleUpdate {
        old_data_if_available: Option<Role>,
        new: Role,
    },
    GuildUpdate {
        old_data_if_available: Option<Guild>,
        new_but_incomplete: PartialGuild,
    },
    MessageDelete {
        channel_id: ChannelId,
        deleted_message_id: MessageId,
        guild_id: Option<GuildId>,
    },
    MessageDeleteBulk {
        channel_id: ChannelId,
        multiple_deleted_messages_ids: Vec<MessageId>,
        guild_id: Option<GuildId>,
    },
    MessageUpdate {
        old_if_available: Option<Message>,
        new: Option<Message>,
        event: serenity::model::event::MessageUpdateEvent,
    },
    ReactionRemoveAll {
        channel_id: ChannelId,
        removed_from_message_id: MessageId,
    },
    UserUpdate {
        old_data: CurrentUser,
        new: CurrentUser,
    },
    VoiceUpdate {
        old: Option<VoiceState>,
        new: VoiceState,
    },
    WebhookUpdate {
        guild_id: GuildId,
        belongs_to_channel_id: ChannelId,
    },
    VoiceServerUpdate(VoiceServerUpdateEvent),
    Resume(ResumedEvent),
    // GuildUnavailable(GuildId),
    GuildMembersChunk(GuildMembersChunkEvent),
    TypingStart(TypingStartEvent),
    PresenceReplace(Vec<Presence>),
    PresenceUpdate(Presence),
}

pub(crate) fn as_snowflake(v: &Value) -> Option<u64> {
    v.as_u64()
        .or_else(|| v.as_str().and_then(|v| v.parse().ok()))
}

pub(crate) fn get_snowflake(v: &Value, k: &str) -> Option<u64> {
    v.get(k).and_then(as_snowflake)
}

fn to_reaction(v: &Value) -> Option<ReactionType> {
    v.as_char().map_or_else(
        || {
            get_snowflake(v, "id").map(|id| ReactionType::Custom {
                id: EmojiId::new(id),
                animated: v.get_bool("animated").unwrap_or_default(),
                name: v.get_str("name").map(ToString::to_string),
            })
        },
        |c| Some(ReactionType::Unicode(c.to_string())),
    )
}
pub(crate) fn to_reactions(v: &Value) -> Option<Vec<ReactionType>> {
    v.as_array()
        .map(|a| a.iter().filter_map(to_reaction).collect::<Vec<_>>())
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn to_reaction_test() {
        let v = literal!("X");
        assert!(to_reaction(&v).is_some());
        let v = literal!("😀");
        assert!(to_reaction(&v).is_some());
        let v = literal!({"id": 42});
        assert!(to_reaction(&v).is_some());
        let v = literal!({"id": "42"});
        assert!(to_reaction(&v).is_some());
        let v = literal!({"id": "42", "animated": true});
        assert!(to_reaction(&v).is_some());
        let v = literal!({"id": "42", "animated": false, "name": "badger"});
        assert!(to_reaction(&v).is_some());
    }
    #[test]
    fn to_reactions_test() {
        let v = literal!(["X"]);
        assert!(to_reactions(&v).is_some());
        let v = literal!(["😀"]);
        assert!(to_reactions(&v).is_some());
        let v = literal!([{"id": 42}]);
        assert!(to_reactions(&v).is_some());
        let v = literal!([{"id": "42"}]);
        assert!(to_reactions(&v).is_some());
        let v = literal!([{"id": "42", "animated": true}]);
        assert!(to_reactions(&v).is_some());
        let v = literal!([{"id": "42", "animated": false, "name": "badger"}]);
        assert!(to_reactions(&v).is_some());
        let v = literal!([{"id": "42", "animated": false, "name": "badger"}, "😀"]);
        assert!(to_reactions(&v).is_some());
    }
    #[test]
    fn as_snowflake_test() {
        assert_eq!(as_snowflake(&Value::from("42")), Some(42));
        assert_eq!(as_snowflake(&Value::from(42)), Some(42));
        assert_eq!(as_snowflake(&Value::from("snot")), None);
        assert_eq!(as_snowflake(&Value::from(42.0)), None);
        assert_eq!(as_snowflake(&Value::from("42.0")), None);
        assert_eq!(as_snowflake(&Value::from(vec![42])), None);
        assert_eq!(as_snowflake(&Value::from("[42]")), None);
    }

    #[test]
    fn get_snowflake_test() {
        let v = literal!({"k": 42});
        assert_eq!(get_snowflake(&v, "k"), Some(42));
        let v = literal!({"k": "42"});
        assert_eq!(get_snowflake(&v, "k"), Some(42));

        let v = literal!({"k": "snot"});
        assert_eq!(get_snowflake(&v, "k"), None);
        let v = literal!({"k": 42.0});
        assert_eq!(get_snowflake(&v, "k"), None);
        let v = literal!({"k": "42.0"});
        assert_eq!(get_snowflake(&v, "k"), None);
        let v = literal!({"k": [42]});
        assert_eq!(get_snowflake(&v, "k"), None);
        let v = literal!(42);
        assert_eq!(get_snowflake(&v, "k"), None);
        let v = literal!({"k": 42});
        assert_eq!(get_snowflake(&v, "x"), None);
    }

    #[test]
    fn intents() {
        assert_eq!(
            GatewayIntents::from(Intents::Guilds),
            GatewayIntents::GUILDS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildMembers),
            GatewayIntents::GUILD_MEMBERS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildModeration),
            GatewayIntents::GUILD_MODERATION
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildEmojis),
            GatewayIntents::GUILD_EMOJIS_AND_STICKERS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildIntegrations),
            GatewayIntents::GUILD_INTEGRATIONS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildWebHooks),
            GatewayIntents::GUILD_WEBHOOKS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildInvites),
            GatewayIntents::GUILD_INVITES
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildVoiceStates),
            GatewayIntents::GUILD_VOICE_STATES
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildPresence),
            GatewayIntents::GUILD_PRESENCES
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildMessages),
            GatewayIntents::GUILD_MESSAGES
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildMessageReactions),
            GatewayIntents::GUILD_MESSAGE_REACTIONS
        );
        assert_eq!(
            GatewayIntents::from(Intents::GuildMessageTyping),
            GatewayIntents::GUILD_MESSAGE_TYPING
        );
        assert_eq!(
            GatewayIntents::from(Intents::DirectMessages),
            GatewayIntents::DIRECT_MESSAGES
        );
        assert_eq!(
            GatewayIntents::from(Intents::DirectMessageReactions),
            GatewayIntents::DIRECT_MESSAGE_REACTIONS
        );
        assert_eq!(
            GatewayIntents::from(Intents::DirectMessageTyping),
            GatewayIntents::DIRECT_MESSAGE_TYPING
        );
        assert_eq!(
            GatewayIntents::from(Intents::MessageContent),
            GatewayIntents::MESSAGE_CONTENT
        );
        assert_eq!(
            GatewayIntents::from(Intents::ScheduledEvents),
            GatewayIntents::GUILD_SCHEDULED_EVENTS
        );
        assert_eq!(GatewayIntents::from(Intents::All), GatewayIntents::all());
        assert_eq!(
            GatewayIntents::from(Intents::NonPrivileged),
            GatewayIntents::non_privileged()
        );
    }
}
