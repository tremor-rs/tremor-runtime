use crate::{
    ast::{Expr, Fields, ImutExpr, Invocable, Invoke, List, NodeId, Record},
    prelude::*,
    CustomFn, NodeMeta,
};

fn v(s: &'static str) -> super::ImutExpr<'static> {
    super::ImutExpr::Literal(super::Literal {
        mid: NodeMeta::dummy(),
        value: Value::from(s),
    })
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
