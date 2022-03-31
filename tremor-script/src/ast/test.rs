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

use super::*;
use crate::{CustomFn, NodeMeta};

fn v(s: &'static str) -> ImutExpr<'static> {
    ImutExpr::literal(NodeMeta::dummy(), Value::from(s))
}

fn p() -> ImutExpr<'static> {
    ImutExpr::Path(Path::State(StatePath {
        mid: NodeMeta::dummy(),
        segments: vec![],
    }))
}

#[test]
fn record() {
    let f1 = super::Field {
        mid: NodeMeta::dummy(),
        name: "snot".into(),
        value: v("badger"),
    };
    let f2 = super::Field {
        mid: NodeMeta::dummy(),
        name: "badger".into(),
        value: v("snot"),
    };

    let r = super::Record {
        base: crate::Object::new(),
        mid: NodeMeta::dummy(),
        fields: vec![f1, f2],
    };

    assert_eq!(r.cloned_field_expr("snot"), Some(v("badger")));
    assert_eq!(r.cloned_field_expr("nots"), None);

    let lit = r.cloned_field_literal("badger");
    assert_eq!(lit.as_str(), Some("snot"));
    assert_eq!(r.cloned_field_expr("adgerb"), None);
}

#[test]
fn as_record() {
    let i = v("snot");
    assert!(i.as_record().is_none());
    let i = ImutExpr::Record(Record {
        mid: Box::new(i.meta().clone()),
        base: Object::new(),
        fields: Fields::new(),
    });
    assert!(i.as_record().is_some());
}

#[test]
fn as_list() {
    let i = v("snot");
    assert!(i.as_list().is_none());
    let i = ImutExpr::List(List {
        mid: Box::new(i.meta().clone()),
        exprs: vec![],
    });
    assert!(i.as_list().is_some());
}
#[test]
fn as_invoke() {
    let invocable = Invocable::Tremor(CustomFn {
        name: "f".into(),
        body: Vec::new(),
        args: Vec::new(),
        open: false,
        locals: 0,
        is_const: false,
        inline: false,
    });
    let i = Invoke {
        mid: NodeMeta::dummy(),
        node_id: NodeId {
            module: Vec::new(),
            id: "fun".to_string(),
        },
        invocable,
        args: Vec::new(),
    };
    assert!(Expr::Imut(v("snut")).as_invoke().is_none());
    let e = ImutExpr::Invoke(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke1(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke2(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
    let e = ImutExpr::Invoke3(i.clone());
    assert!(Expr::Imut(e).as_invoke().is_some());
}

#[test]
fn as_imut() {
    let i = ImutExpr::List(List {
        mid: NodeMeta::dummy(),
        exprs: vec![],
    });
    assert!(Expr::Imut(i).as_imut().is_ok());
    assert!(Expr::Drop {
        mid: NodeMeta::dummy()
    }
    .as_imut()
    .is_err());
}

#[test]
fn try_into_value() -> crate::errors::Result<()> {
    let aggr_reg = crate::registry::aggr();
    let reg = &*crate::FN_REGISTRY.read()?;
    let helper = Helper::new(reg, &aggr_reg);

    let val = v("value");
    assert!(val.try_into_value(&helper).is_ok());

    let list = ImutExpr::List(List {
        mid: NodeMeta::dummy(),
        exprs: vec![],
    });
    assert!(list.try_into_value(&helper).is_ok());

    let path = p();
    assert!(path.try_into_value(&helper).is_err());

    Ok(())
}
#[test]
fn str_lit_as_str() {
    assert_eq!(StrLitElement::Lit("string".into()).as_str(), Some("string"));
    assert_eq!(StrLitElement::Expr(v("string")).as_str(), Some("string"));
    assert_eq!(StrLitElement::Expr(p()).as_str(), None);
}
