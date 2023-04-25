// Copyright 2022, The Tremor Team
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

//! This crate provides declarative macros used in the tremor runtime

#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::unnecessary_unwrap,
    clippy::pedantic,
    missing_docs
)]

use proc_macro::TokenStream;
use quote::quote;
use syn::Data;

#[proc_macro_derive(SocketServer)]
/// Derive macro for the `SocketServer` trait
/// # Panics
/// Panics if the macro fails to execute
pub fn socket_server(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);

    match ast.data {
        Data::Struct(_) => {
            let name = ast.ident;
            let (generics_impl, generics_type, generics_where) = ast.generics.split_for_impl();

            let gen = quote! {
                #[async_trait::async_trait]
                impl #generics_impl SocketServer for #name #generics_type #generics_where {
                    async fn listen(&mut self, address: &str, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("SocketServer::listen".to_string()).into())
                    }

                    async fn close(&mut self, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("SocketServer::close".to_string()).into())
                    }
                }
            };

            TokenStream::from(gen)
        }
        // ALLOW: Panicking is the correct way to report an error in a procedural macro
        Data::Enum(_) | Data::Union(_) => panic!("SocketServer can only be derived for structs"),
    }
}

#[proc_macro_derive(FileIo)]
/// A declarative macro for generating an empty `FileIo` implementation
/// # Panics
/// Panics if the macro fails to execute
pub fn file_io(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    match ast.data {
        Data::Struct(_) => {
            let name = ast.ident;
            let (generics_impl, generics_type, generics_where) = ast.generics.split_for_impl();

            let gen = quote! {
                #[async_trait::async_trait]
                impl #generics_impl FileIo for #name #generics_type #generics_where {
                    async fn open(&mut self, path: &str, handle: &str, mode: crate::connectors::traits::FileMode, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("FileIo::open".to_string()).into())
                    }
                    async fn close(&mut self, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("FileIo::close".to_string()).into())
                    }
                }
            };

            TokenStream::from(gen)
        }
        Data::Enum(_) | Data::Union(_) => {
            // ALLOW: Panicking is the correct way to report an error in a procedural macro
            panic!("FileIo can only be derived for structs");
        }
    }
}

#[proc_macro_derive(SocketClient)]
/// A declarative macro for generating an empty `SocketClient` implementation
/// # Panics
/// Panics if the macro fails to execute
pub fn socket_client(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    match ast.data {
        Data::Struct(_) => {
            let name = ast.ident;
            let (generics_impl, generics_type, generics_where) = ast.generics.split_for_impl();

            let gen = quote! {
                #[async_trait::async_trait]
                impl #generics_impl SocketClient for #name #generics_type #generics_where {
                    async fn connect_socket(&mut self, address: &str, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("SocketClient::connect".to_string()).into())
                    }

                    async fn close(&mut self, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("SocketClient::close".to_string()).into())
                    }
                }
            };

            TokenStream::from(gen)
        }
        Data::Enum(_) | Data::Union(_) => {
            // ALLOW: Panicking is the correct way to report an error in a procedural macro
            panic!("SocketClient can only be derived for structs");
        }
    }
}

#[proc_macro_derive(QueueSubscriber)]
/// A declarative macro for generating an empty `QueueSubscriber` implementation
/// # Panics
/// Panics if the macro fails to execute
pub fn queue_subscriber(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    match ast.data {
        Data::Struct(_) => {
            let name = ast.ident;
            let (generics_impl, generics_type, generics_where) = ast.generics.split_for_impl();

            let gen = quote! {
                #[async_trait::async_trait]
                impl #generics_impl QueueSubscriber for #name #generics_type #generics_where {
                    async fn subscribe(&mut self, topics: Vec<String>, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("QueueSubscriber::subscribe".to_string()).into())
                    }

                    async fn unsubscribe(&mut self, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("QueueSubscriber::unsubscribe".to_string()).into())
                    }
                }
            };

            TokenStream::from(gen)
        }
        Data::Enum(_) | Data::Union(_) => {
            // ALLOW: Panicking is the correct way to report an error in a procedural macro
            panic!("QueueSubscriber can only be derived for structs");
        }
    }
}

#[proc_macro_derive(DatabaseWriter)]
/// A declarative macro for generating an empty `DatabaseWriter` implementation
/// # Panics
/// Panics if the macro fails to execute
pub fn database_writer(input: TokenStream) -> TokenStream {
    let ast = syn::parse_macro_input!(input as syn::DeriveInput);
    match ast.data {
        Data::Struct(_) => {
            let name = ast.ident;
            let (generics_impl, generics_type, generics_where) = ast.generics.split_for_impl();

            let gen = quote! {
                #[async_trait::async_trait]
                impl #generics_impl DatabaseWriter for #name #generics_type #generics_where {
                    async fn open_table(&mut self, table: &str, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("DatabaseWriter::open_table".to_string()).into())
                    }

                    async fn close_table(&mut self, handle: &str, ctx: &crate::connectors::prelude::SinkContext) -> Result<()> {
                        Err(crate::errors::ErrorKind::NotImplemented("DatabaseWriter::close_table".to_string()).into())
                    }
                }
            };

            TokenStream::from(gen)
        }
        Data::Enum(_) | Data::Union(_) => {
            // ALLOW: Panicking is the correct way to report an error in a procedural macro
            panic!("DatabaseWriter can only be derived for structs");
        }
    }
}
