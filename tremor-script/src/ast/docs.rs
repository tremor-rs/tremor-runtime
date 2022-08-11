use simd_json::ValueType;

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

/// Documentation from constant
#[derive(Debug, Clone, PartialEq)]
pub struct ConstDoc {
    /// Constant name
    pub name: String,
    /// Constant documentation
    pub doc: Option<String>,
    /// Constant value type
    pub value_type: ValueType,
}

impl ToString for ConstDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}

*type*: {:?}

{}
"#,
            self.name,
            self.value_type,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from function
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FnDoc {
    /// Function name
    pub name: String,
    /// Function arguments
    pub args: Vec<String>,
    /// Function documentation
    pub doc: Option<String>,
    /// Whether the function is open or not
    // TODO clarify what open exactly is
    pub open: bool,
}

impl ToString for FnDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}({})

{}
"#,
            self.name,
            self.args.join(", "),
            self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq, Serialize, Eq)]
pub struct ModDoc {
    /// Module name
    pub name: String,
    /// Module documentation
    pub doc: Option<String>,
}

impl ModDoc {
    /// Prints the module documentation
    #[must_use]
    pub fn print_with_name(&self, name: &str) -> String {
        format!(
            r#"
# {}

{}
"#,
            name,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a query statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryDoc {
    /// Statment name
    pub name: String,
    /// Statment documentation
    pub doc: Option<String>,
}

impl ToString for QueryDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}

{}
"#,
            self.name,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a flow statement
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowDoc {
    /// Statment name
    pub name: String,
    /// Statement documentation
    pub doc: Option<String>,
}

impl ToString for FlowDoc {
    fn to_string(&self) -> String {
        format!(
            r#"
### {}

{}
"#,
            self.name,
            &self.doc.clone().unwrap_or_default()
        )
    }
}

/// Documentation from a module
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Docs {
    /// Constants
    pub consts: Vec<ConstDoc>,
    /// Functions
    pub fns: Vec<FnDoc>,
    /// Querys
    pub queries: Vec<QueryDoc>,
    /// Flows
    pub flows: Vec<FlowDoc>,
    /// Module level documentation
    pub module: Option<ModDoc>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn const_doc() {
        let c = ConstDoc {
            name: "const test".into(),
            doc: Some("hello".into()),
            value_type: ValueType::Null,
        };
        assert_eq!(
            c.to_string(),
            r#"
### const test

*type*: Null

hello
"#
        );
    }

    #[test]
    fn fn_doc() {
        let c = FnDoc {
            name: "fn_test".into(),
            args: vec!["snot".into(), "badger".into()],
            doc: Some("hello".into()),
            open: false,
        };
        assert_eq!(
            c.to_string(),
            r#"
### fn_test(snot, badger)

hello
"#
        );
    }

    #[test]
    fn mod_doc() {
        let c = ModDoc {
            name: "test mod".into(),
            doc: Some("hello".into()),
        };
        assert_eq!(
            c.print_with_name(&c.name),
            r#"
# test mod

hello
"#
        );
    }

    #[test]
    fn query_doc() {
        let q = QueryDoc {
            name: "test query".into(),
            doc: Some("hello".into()),
        };
        assert_eq!(
            q.to_string(),
            r#"
### test query

hello
"#
        );
    }
}
