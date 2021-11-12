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

use serenity::{
    client::bridge::gateway::GatewayIntents,
    model::{
        channel::{Channel, ChannelCategory, GuildChannel, Message, Reaction, ReactionType},
        event::{GuildMembersChunkEvent, ResumedEvent, TypingStartEvent, VoiceServerUpdateEvent},
        guild::{Emoji, Guild, GuildUnavailable, Member, PartialGuild, Role},
        id::{ChannelId, EmojiId, GuildId, MessageId, RoleId},
        prelude::{CurrentUser, Presence, User, VoiceState},
    },
};
use std::collections::HashMap;
use tremor_value::prelude::*;

#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
pub enum Intents {
    /// Guilds Grouping
    Guilds,
    /// GuildMembers Grouping
    GuildMembers,
    /// GuildBans Grouping
    GuildBans,
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
            Intents::GuildBans => GatewayIntents::GUILD_BANS,
            Intents::GuildEmojis => GatewayIntents::GUILD_EMOJIS,
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
    ChannelDelete(GuildChannel),
    CategoryCreate(ChannelCategory),
    CategoryDelete(ChannelCategory),
    ChannelUpdate {
        old: Option<Channel>,
        new: Channel,
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
        is_new: bool,
    },
    GuildDelete {
        incomplete: GuildUnavailable,
        full: Option<Guild>,
    },
    EmojiUpdate {
        guild_id: GuildId,
        current_state: HashMap<EmojiId, Emoji>,
    },
    IntegrationsUpdate(GuildId),
    MemberAddition {
        guild_id: GuildId,
        new_member: Member,
    },
    MemberRemoval {
        guild_id: GuildId,
        user: User,
        member_data_if_available: Option<Member>,
    },
    MemberUpdate {
        old_if_available: Option<Member>,
        new: Member,
    },
    RoleCreate {
        guild_id: GuildId,
        new: Role,
    },
    RoleDelete {
        guild_id: GuildId,
        removed_role_id: RoleId,
        removed_role_data_if_available: Option<Role>,
    },
    RoleUpdate {
        guild_id: GuildId,
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
    GuildUnavailable(GuildId),
    GuildMembersChunk(GuildMembersChunkEvent),
    TypingStart(TypingStartEvent),
    PresenceReplace(Vec<Presence>),
}

fn to_reaction(v: &Value) -> Option<ReactionType> {
    v.as_char().map_or_else(
        || {
            v.get_u64("id").map(|id| ReactionType::Custom {
                id: EmojiId(id),
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
