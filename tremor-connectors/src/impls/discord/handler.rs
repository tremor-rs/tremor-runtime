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
use super::utils::{as_snowflake, get_snowflake, to_reactions, DiscordMessage};
use crate::channel::{Receiver, Sender};
use serenity::{
    all::{CreateEmbed, CreateEmbedAuthor, CreateEmbedFooter, CreateMessage, EditMember},
    model::{
        channel::{GuildChannel, Message, Reaction},
        event::{
            ChannelPinsUpdateEvent, GuildMemberUpdateEvent, GuildMembersChunkEvent, ResumedEvent,
            TypingStartEvent, VoiceServerUpdateEvent,
        },
        guild::{Emoji, Guild, Member, PartialGuild, Role},
        id::{ChannelId, EmojiId, GuildId, MessageId, RoleId, UserId},
        prelude::{CurrentUser, Presence, Ready, User},
        voice::VoiceState,
    },
    prelude::*,
};
use simd_json::prelude::ValueObjectAccess;
use std::collections::HashMap;
use tokio::task;
use tremor_value::{prelude::*, to_value};

pub(crate) struct Handler {
    pub tx: Sender<Value<'static>>,
    // We don't need a RwLock her but the EventHandler requires functions to take a non
    // mut &self so during initiualisation we got to take the rx - meaning we need to RwLock it
    pub rx: RwLock<Option<Receiver<Value<'static>>>>,
}

impl Handler {
    async fn forward<T>(&self, msg: T)
    where
        T: serde::Serialize,
    {
        let event: Value<'static> = match to_value(msg) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to decode event: {}", e);
                return;
            }
        };
        if let Err(e) = self.tx.send(event).await {
            error!("Failed to forward event: {}", e);
        }
    }
}

#[async_trait::async_trait]
impl EventHandler for Handler {
    // We use the cache_ready event just in case some cache operation is required in whatever use
    // case you have for this.
    async fn cache_ready(&self, ctx: Context, _guilds: Vec<GuildId>) {
        info!("Cache built successfully!");

        if let Some(rx) = (*(self.rx.write().await)).take() {
            task::spawn(async move {
                reply_loop(rx, ctx).await;
            });
        }
    }

    async fn channel_create(&self, _ctx: Context, channel: GuildChannel) {
        self.forward(DiscordMessage::ChannelCreate(channel)).await;
    }

    async fn channel_delete(
        &self,
        _ctx: Context,
        channel: GuildChannel,
        maybe_message: Option<Vec<Message>>,
    ) {
        self.forward(DiscordMessage::ChannelDelete(channel, maybe_message))
            .await;
    }

    async fn channel_pins_update(&self, _ctx: Context, pin: ChannelPinsUpdateEvent) {
        self.forward(pin).await;
    }

    async fn invite_create(&self, _ctx: Context, data: serenity::model::event::InviteCreateEvent) {
        self.forward(data).await;
    }

    async fn invite_delete(&self, _ctx: Context, data: serenity::model::event::InviteDeleteEvent) {
        self.forward(data).await;
    }

    async fn message(&self, _ctx: Context, msg: Message) {
        self.forward(msg).await;
    }

    async fn reaction_add(&self, _ctx: Context, add_reaction: Reaction) {
        self.forward(DiscordMessage::AddReaction(add_reaction))
            .await;
    }

    async fn reaction_remove(&self, _ctx: Context, removed_reaction: Reaction) {
        self.forward(DiscordMessage::RemoveReaction(removed_reaction))
            .await;
    }

    async fn presence_replace(&self, _ctx: Context, p: Vec<Presence>) {
        self.forward(DiscordMessage::PresenceReplace(p)).await;
    }

    async fn presence_update(&self, _ctx: Context, new_data: Presence) {
        self.forward(DiscordMessage::PresenceUpdate(new_data)).await;
    }

    async fn ready(&self, _: Context, ready: Ready) {
        info!("{} is connected!", ready.user.name);
    }

    async fn typing_start(&self, _ctx: Context, e: TypingStartEvent) {
        self.forward(DiscordMessage::TypingStart(e)).await;
    }

    async fn channel_update(&self, _ctx: Context, old: Option<GuildChannel>, new: GuildChannel) {
        self.forward(DiscordMessage::ChannelUpdate { old, new })
            .await;
    }

    async fn guild_ban_addition(&self, _ctx: Context, guild_id: GuildId, user: User) {
        self.forward(DiscordMessage::BanAddition { guild_id, user })
            .await;
    }

    async fn guild_ban_removal(&self, _ctx: Context, guild_id: GuildId, user: User) {
        self.forward(DiscordMessage::BanRemoval { guild_id, user })
            .await;
    }

    async fn guild_create(&self, _ctx: Context, guild: Guild, is_new: Option<bool>) {
        self.forward(DiscordMessage::GuildCreate { guild, is_new })
            .await;
    }

    // async fn guild_delete(&self, _ctx: Context, incomplete: GuildUnavailable, full: Option<Guild>) {
    //     self.forward(DiscordMessage::GuildDelete { incomplete, full })
    //         .await;
    // }

    async fn guild_emojis_update(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        current_state: HashMap<EmojiId, Emoji>,
    ) {
        self.forward(DiscordMessage::EmojiUpdate {
            guild_id,
            current_state,
        })
        .await;
    }

    async fn guild_integrations_update(&self, _ctx: Context, guild_id: GuildId) {
        self.forward(DiscordMessage::IntegrationsUpdate(guild_id))
            .await;
    }

    async fn guild_member_addition(&self, _ctx: Context, new_member: Member) {
        self.forward(DiscordMessage::MemberAddition { new_member })
            .await;
    }

    async fn guild_member_removal(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        user: User,
        member_data_if_available: Option<Member>,
    ) {
        self.forward(DiscordMessage::MemberRemoval {
            guild_id,
            user,
            member_data_if_available,
        })
        .await;
    }

    async fn guild_member_update(
        &self,
        _ctx: Context,
        old_if_available: Option<Member>,
        member: Option<Member>,
        event: GuildMemberUpdateEvent,
    ) {
        self.forward(DiscordMessage::MemberUpdate {
            old_if_available,
            member,
            event,
        })
        .await;
    }

    async fn guild_members_chunk(&self, _ctx: Context, chunk: GuildMembersChunkEvent) {
        self.forward(DiscordMessage::GuildMembersChunk(chunk)).await;
    }

    async fn guild_role_create(&self, _ctx: Context, new: Role) {
        self.forward(DiscordMessage::RoleCreate { new }).await;
    }

    async fn guild_role_delete(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        removed_role_id: RoleId,
        removed_role_data_if_available: Option<Role>,
    ) {
        self.forward(DiscordMessage::RoleDelete {
            guild_id,
            removed_role_id,
            removed_role_data_if_available,
        })
        .await;
    }

    async fn guild_role_update(
        &self,
        _ctx: Context,
        old_data_if_available: Option<Role>,
        new: Role,
    ) {
        self.forward(DiscordMessage::RoleUpdate {
            old_data_if_available,
            new,
        })
        .await;
    }

    async fn guild_update(
        &self,
        _ctx: Context,
        old_data_if_available: Option<Guild>,
        new_but_incomplete: PartialGuild,
    ) {
        self.forward(DiscordMessage::GuildUpdate {
            old_data_if_available,
            new_but_incomplete,
        })
        .await;
    }

    async fn message_delete(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        deleted_message_id: MessageId,
        guild_id: Option<GuildId>,
    ) {
        self.forward(DiscordMessage::MessageDelete {
            channel_id,
            deleted_message_id,
            guild_id,
        })
        .await;
    }

    async fn message_delete_bulk(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        multiple_deleted_messages_ids: Vec<MessageId>,
        guild_id: Option<GuildId>,
    ) {
        self.forward(DiscordMessage::MessageDeleteBulk {
            channel_id,
            multiple_deleted_messages_ids,
            guild_id,
        })
        .await;
    }

    async fn message_update(
        &self,
        _ctx: Context,
        old_if_available: Option<Message>,
        new: Option<Message>,
        event: serenity::model::event::MessageUpdateEvent,
    ) {
        self.forward(DiscordMessage::MessageUpdate {
            old_if_available,
            new,
            event,
        })
        .await;
    }

    async fn reaction_remove_all(
        &self,
        _ctx: Context,
        channel_id: ChannelId,
        removed_from_message_id: MessageId,
    ) {
        self.forward(DiscordMessage::ReactionRemoveAll {
            channel_id,
            removed_from_message_id,
        })
        .await;
    }

    async fn resume(&self, _ctx: Context, resume: ResumedEvent) {
        self.forward(DiscordMessage::Resume(resume)).await;
    }

    async fn user_update(&self, _ctx: Context, old_data: Option<CurrentUser>, new: CurrentUser) {
        if let Some(old_data) = old_data {
            self.forward(DiscordMessage::UserUpdate { old_data, new })
                .await;
        } else {
            self.forward(DiscordMessage::UserUpdate {
                old_data: new.clone(),
                new,
            })
            .await;
        }
    }

    async fn voice_server_update(&self, _ctx: Context, update: VoiceServerUpdateEvent) {
        self.forward(DiscordMessage::VoiceServerUpdate(update))
            .await;
    }

    async fn voice_state_update(&self, _ctx: Context, old: Option<VoiceState>, new: VoiceState) {
        self.forward(DiscordMessage::VoiceUpdate { old, new }).await;
    }

    async fn webhook_update(
        &self,
        _ctx: Context,
        guild_id: GuildId,
        belongs_to_channel_id: ChannelId,
    ) {
        self.forward(DiscordMessage::WebhookUpdate {
            guild_id,
            belongs_to_channel_id,
        })
        .await;
    }
}

#[allow(clippy::too_many_lines)]
async fn reply_loop(mut rx: Receiver<Value<'static>>, ctx: Context) {
    while let Some(reply) = rx.recv().await {
        if let Some(reply) = reply.get("guild") {
            let guild = if let Some(id) = get_snowflake(reply, "id") {
                GuildId::new(id)
            } else {
                error!("guild `id` missing");
                continue;
            };

            if let Some(member) = reply.get("member") {
                if let Some(id) = get_snowflake(member, "id") {
                    let user = UserId::new(id);
                    let current_member = match guild.member(&ctx, user).await {
                        Ok(current_member) => current_member,
                        Err(e) => {
                            error!("Member error: {}", e);
                            continue;
                        }
                    };
                    if let Some(to_remove) = member.get_array("remove_roles") {
                        let to_remove: Vec<_> = to_remove
                            .iter()
                            .filter_map(|v| as_snowflake(v).map(RoleId::new))
                            .collect();
                        if let Err(e) = current_member.remove_roles(&ctx, &to_remove).await {
                            error!("Role removal error: {}", e);
                        };
                    }

                    if let Some(to_roles) = member.get_array("add_roles") {
                        let to_roles: Vec<_> = to_roles
                            .iter()
                            .filter_map(|v| as_snowflake(v).map(RoleId::new))
                            .collect();
                        if let Err(e) = current_member.add_roles(&ctx, &to_roles).await {
                            error!("Role add error: {}", e);
                        };
                    }
                    let mut em = EditMember::default();
                    if let Some(mute) = member.get_bool("mute") {
                        em = em.mute(mute);
                    };
                    if let Some(deaf) = member.get_bool("deaf") {
                        em = em.deafen(deaf);
                    };
                    let r = guild.edit_member(&ctx, id, em).await;
                    if let Err(e) = r {
                        error!("Mute/Deafen error: {}", e);
                    };
                }
            }
        }
        if let Some(reply) = reply.get("message") {
            let channel = if let Some(id) = get_snowflake(reply, "channel_id") {
                ChannelId::new(id)
            } else {
                error!("channel_id missing");
                continue;
            };

            if let Some(reply) = reply.get("update") {
                if let Some(message_id) = get_snowflake(reply, "message_id") {
                    let message = match channel.message(&ctx, message_id).await {
                        Ok(message) => message,
                        Err(e) => {
                            error!("Message error: {}", e);
                            continue;
                        }
                    };

                    if let Some(reactions) = reply.get("add_reactions").and_then(to_reactions) {
                        for r in reactions {
                            if let Err(e) = message.react(&ctx, r).await {
                                error!("Message reaction error: {}", e);
                            };
                        }
                    }
                }
            }

            if let Some(reply) = reply.get("send") {
                let mut created_message = CreateMessage::default();
                // Normal content
                if let Some(content) = reply.get_str("content") {
                    created_message = created_message.content(content);
                };
                // Reference to another message
                if let Some(reference_message) = get_snowflake(reply, "reference_message") {
                    let reference_channel =
                        get_snowflake(reply, "reference_channel").map_or(channel, ChannelId::new);
                    created_message = created_message
                        .reference_message((reference_channel, MessageId::new(reference_message)));
                };
                if let Some(tts) = reply.get_bool("tts") {
                    created_message = created_message.tts(tts);
                };
                if let Some(embed) = reply.get("embed") {
                    let mut created_embed = CreateEmbed::default();
                    if let Some(author) = embed.get("author") {
                        if let Some(name) = author.get_str("name") {
                            let mut create_embed_author = CreateEmbedAuthor::new(name);
                            if let Some(icon_url) = author.get_str("icon_url") {
                                create_embed_author = create_embed_author.icon_url(icon_url);
                            };
                            if let Some(url) = author.get_str("url") {
                                create_embed_author = create_embed_author.url(url);
                            };
                            created_embed = created_embed.author(create_embed_author);
                        }
                    };
                    if let Some(colour) = embed.get_u64("colour") {
                        created_embed = created_embed.colour(colour);
                    };
                    if let Some(description) = embed.get_str("description") {
                        created_embed = created_embed.description(description);
                    };
                    if let Some(fields) = embed.get_array("fields") {
                        created_embed = created_embed.fields(fields.iter().filter_map(|v| {
                            let name = v.get_str("name")?;
                            let value = v.get_str("value")?;
                            let inline = v.get_bool("inline").unwrap_or_default();
                            Some((name, value, inline))
                        }));
                    };
                    if let Some(footer) = embed.get("footer") {
                        if let Some(text) = footer.as_str() {
                            let mut created_embed_footer = CreateEmbedFooter::new(text);
                            if let Some(text) = footer.get_str("text") {
                                created_embed_footer = created_embed_footer.text(text);
                            };
                            if let Some(icon_url) = footer.get_str("icon_url") {
                                created_embed_footer = created_embed_footer.icon_url(icon_url);
                            };
                            created_embed = created_embed.footer(created_embed_footer);
                        };
                    };
                    created_message = created_message.embed(created_embed);
                };
                if let Some(reactions) = reply.get("reactions").and_then(to_reactions) {
                    created_message = created_message.reactions(reactions);
                };
                if let Err(e) = channel.send_message(&ctx, created_message).await {
                    error!("Discord send error: {}", e);
                };
            };
        }
    }
}
