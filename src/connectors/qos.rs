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

use crate::sink::prelude::*;

pub(crate) fn ack(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_ack())
}

pub(crate) fn fail(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_fail())
}

pub(crate) fn close(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_trigger())
}

pub(crate) fn open(event: &mut Event) -> sink::Reply {
    sink::Reply::Insight(event.insight_restore())
}
