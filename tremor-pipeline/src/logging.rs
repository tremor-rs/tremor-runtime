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

//use beef::Cow;
//use halfbrown::HashMap;
use crate::{errors::Result};
use simd_json::ValueAccess;
use tremor_script::{registry::{Registry, FResult, mfa, FunctionError}, TremorFn, EventContext, Value, TremorFnWrapper};
/// Install's common functions into a registry
///
/// # Errors
///  * if we can't install extensions

#[derive(Clone, Debug, Default)]
struct PluggableLoggingFm {}
impl TremorFn for PluggableLoggingFm {
    fn invoke<'event, 'c>(
        &self,
        _ctx: &'c EventContext,
        args: &[&Value<'event>]
    ) -> FResult<Value<'event>> {
        let this_mfa = || mfa("logging", "info", args.len());
        if args.is_empty() {
            return Err(FunctionError::BadArity {
                mfa: this_mfa(),
                calling_a: args.len(),
            });
        }
        if let Some((format, arg_stack)) = args
            .split_first()
            .and_then(|(f, arg_stack)| Some((f.as_str()?, arg_stack)))
        {
			let out = if arg_stack.len() > 1 {
				let arg_stack: Vec<Value> = arg_stack.iter().map(|v| v.clone_static()).collect();
				positional_formatting(format, &Value::Array(arg_stack))
			} else if arg_stack.len() == 1 {
				let mut arg_stack: Vec<&&Value> = arg_stack.iter().collect();
				let &arg_stack = arg_stack.pop().unwrap();
				//todo manage when data is not object
				if let Some(_) = arg_stack.as_object() { // retrieve format args as a hashmap
					named_formatting(format, arg_stack)
				} else if let Some(_) = arg_stack.as_array() {
					positional_formatting(format, arg_stack)
				} else {
					positional_formatting(format, &Value::Array(vec![arg_stack.clone()]))
				}
			} else { // arg_stack.len() < 1 (== 0)
				positional_formatting(format, &Value::Array(vec![])) // `arg_stack` is empty
			};

			match out {
				Ok(out_ok) => {
					info!("{}", out_ok.clone());
					dbg!(out_ok.clone());
					dbg!(args.clone());
					Ok(out_ok)
				},
				Err(a) => {
					Err(dbg!(a))
					//out
				}
			}
		} else {
			Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "expected 1st parameter to format to be a format specifier e.g. to print a number use `string::format(\"{}\", 1)`".into()})
		}
    }

    fn boxed_clone(&self) -> Box<dyn TremorFn> {
        Box::new(self.clone())
    }
    fn arity(&self) -> std::ops::RangeInclusive<usize> {
        1_usize..=usize::max_value()
    }
    fn is_const(&self) -> bool {
        true
    }

    fn valid_arity(&self, n: usize) -> bool {
        self.arity().contains(&n)
    }
}


/// Exact copy from String
fn positional_formatting<'event>(format: &str, arg_stack: &Value) -> FResult<Value<'event>> {

	let this_mfa = || mfa("logging", "TODO", 0); // because we now know the function

	//let mut arg_stack = arg_stack.as_array().unwrap().clone();
	let mut arg_stack: Vec<&Value> = arg_stack.as_array().unwrap().iter().rev().collect();

	let mut out = String::with_capacity(format.len());
	let mut iter = format.chars().enumerate();
	while let Some((pos, char)) = iter.next() {
		match char {
			'\\' => if let Some((_, c)) = iter.next() {
				if c != '{' && c != '}' {
					out.push('\\');
				};
				out.push(c);
			} else {
				return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("bad escape sequence at {}", pos)});
			}
			'{' => match iter.next() {
				Some((_, '}')) => if let Some(arg) = arg_stack.pop() {
					out.push_str(&match format_tremor_value(arg) {
						Ok(s) => s,
						Err(e) => return Err(FunctionError::RuntimeError{ mfa: this_mfa(), error: e.to_string()})
					});
				} else {
						return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {} can not be filled", pos)});
				},
				Some((_, '{')) => {
					out.push('{');
				}
				_ => {
					return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {} is invalid. If you want to use `{{` as a literal in the string, you need to escape it with `{{{{`", pos)})
				}
			},
			'}' => if let Some((_, '}')) =  iter.next() {
				out.push('}');
			} else {
				return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {} is invalid. You have to terminate `}}` with another `}}` to escape it", pos)});
			},
			c => out.push(c),
		}
	}
	// if arg_stack.is_empty() {
	Ok(Value::from(out))
	// } else {
	// 	Err(FunctionError::RuntimeError{mfa: this_mfa(), error: "too many parameters passed. Ensure that you have the same number of {} in your format string".into()})
	// }
}


fn named_formatting<'event>(format: &str, arg_stack: &Value) -> FResult<Value<'event>> {
	let this_mfa = || mfa("logging", "TODO", 0); // because we now know the function

	let format_fields = arg_stack.as_object().unwrap();

	let mut out = String::with_capacity(format.len());
	// out.push('['); out.push_str(format); out.push(']');
	let mut iter = format.chars().enumerate();
	while let Some((pos, char)) = iter.next() {
		match char {
			'\\' =>
				if let Some((_, c)) = iter.next() {
					if c != '{' && c != '}' {
						out.push('\\');
					};
					out.push(c);
				} else {
					return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("bad escape sequence at {pos}: cannot end string with escape character")});
				},
			'{' => {
				let mut identifier = String::new(); // named placeholder "format specifier"
				let mut init = true;
				loop { // reading the identifier name
					match iter.next() {
						Some((_, '{')) => {
							if init {
								out.push('{');
								break;
							} else {
								return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {pos} is invalid. You have to terminate `{{` with another `{{` to escape it")});
							}
						},
						Some((_, '}')) => {
							match format_fields.get(identifier.as_str()) {
								Some(value) =>  {
									out.push_str(format_tremor_value(value).unwrap().as_str());
								},
								None => return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format name at {pos} cannot be found among* the `Value:Object` given. Cannot insert field mapped with name {identifier} because it was not found in the given hashmap of args. *This is sus")})
							}
							break;
						},
						Some((_, c)) => {
							init = false;
							identifier.push(c);
						},
						None => return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {pos} is invalid. Identifier truncated. No ending `}}` found.")})
					}
				}
			}
			'}' =>
				if let Some((_, '}')) = iter.next() {
					out.push('}');
				} else {
					return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the format specifier at {pos} is invalid. You have to terminate `}}` with another `}}` to escape it")});
				},
			c => out.push(c)
		}
		/*
		return Err(FunctionError::RuntimeError{mfa: this_mfa(),
			error: format!("the format specifier at {pos} is invalid. It only support named formatting, and not positional formatting.") +
			&format!("You need to place an identifier to name the value to be retrieved and formatted")
		});
		//todo you are supposed to handle both formatting by value and positional formatting
		//todo but in this case only positional formatting is handled
		return Err(FunctionError::RuntimeError{mfa: this_mfa(), error: format!("the arguments passed to the format function are less than the `{{}}` specifiers in the format string. The placeholder at {pos} can not be filled")});
		*/
	}
	Ok(Value::from(out))
	
	//TODO add variable argument to sub-record
	// Ok(Value::from("temp placeholder"))
	//dbg!(_ctx.origin_uri().unwrap().to_string());
}


fn format_tremor_value(value: &Value) -> Result<String> {
	let mut result = String::new();
	let error_result =
	Err("Not Implemented. Could not deserialize from `Tremor::Value` to any supporteed rust types.\nSupported types are either &str, i32, bool, Vec, bytes, HashMap".into());

	
	//todo: use value.value_type() instead of trying to convert and fail
	/*ValueType::Null;
    ValueType::Bool;
    ValueType::I64;
    ValueType::I128;
    ValueType::U64;
    ValueType::U128;
    ValueType::F64;
	ValueType::String;
	ValueType::Array;
	ValueType::Object;
    // ValueType::Extended(ExtendedValueType) // Extended are not supported as tremor value core types
	ValueType::Custom("bytes");


	*/

	if let Some(string) = value.as_str() {
		result.push_str(string);
		return Ok(result);
	}

	if let Some(integer32) = value.as_i32() {
		result.push_str(&integer32.to_string());
		return Ok(result);
	}

	if let Some(boolean) = value.as_bool() {
		result.push_str(&boolean.to_string());
		return Ok(result)
	}

	if let Some(array) = value.as_array() {
		let mut array = array.clone();
		result.push('[');
		let mut last = String::new();
		if !array.is_empty() {
			// Keeping the last value of the array for a nice formatting
			// We will be able to get ["hello", "nice", "last"] instead of ["hello", "nice", "last", ] 
			last = match format_tremor_value(&array.pop().unwrap()) {
				Ok(v) => v,
				Err(_) => {return error_result;}
			};
		}
		for v in array.iter() {
			result.push_str(
				&match format_tremor_value(&v.clone_static()) {
					Ok(v) => v,
					Err(_) => {return error_result;}
				});
			result.push_str(", ");
		}
		// last element
		result.push_str(&last);
		result.push(']');

		return Ok(result);
	}

	if let Some(_bytes) = value.as_bytes() {
		// result.push_str(match str::from_utf8(bytes) {
		// 	Ok(v) => v,
		// 	Err(_) => 
		// })
		todo!("bytes");
	}

	if let Some(obj) = value.as_object() {
		let mut obj = obj.clone();
		result.push('{');
		let mut last = String::new();
		if !obj.is_empty() {
			// Keeping the last pair of key:value of the object for a nice formatting
			// We will be able to get ["hello", "nice", "last"] instead of ["hello", "nice", "last", ]
			let p = obj.iter().last().unwrap();
			let p = (p.0.clone().unwrap_borrowed(),
					&match format_tremor_value(&p.1.clone_static()) {
						Ok(v) => v,
						Err(_) => {return error_result;}
					}
				);
			obj.remove(p.0);
			let mut p2 = String::new();
			p2.push_str(&p.0);
			p2.push_str(": ");
			p2.push_str(p.1);
			last = p2;
		}

		for pair in obj.iter() {
			let pair = (pair.0.clone().unwrap_borrowed(), pair.1);
			result.push_str(pair.0);//  key
			result.push_str(": ");	//  sep
			result.push_str(				// value
				&match format_tremor_value(&pair.1.clone_static()) {
					Ok(v) => v,
					Err(_) => {return error_result;}
				});
			result.push_str(", ");	//pair sep
		}
		// last element
		result.push_str(&last);
		result.push('}');

		return Ok(result);
	}

	return error_result;
}



///Todo
pub fn load(reg: &mut Registry) -> Result<()> {
    reg
    // .insert(tremor_fn!(
	// 	logging|warn(_context, msg:String) {
	// 		warn!("{msg}");
	// 		println!("AAAAAAAAH! {} AAAAAAAAAAAAAAAAAAAAH", _context.origin_uri.unwrap().to_string());
	// 		Ok(Value::from(msg.to_string()))
	// 	}
	// ))
    // .insert(tremor_fn!(
	// 	logging|error(_context, msg:String) {
	// 		error!("{msg}");
	// 		Ok(Value::from(msg.to_string()))
	// 	}
	// ))
    // .insert(tremor_fn!(
	// 	logging|debug(_context, msg:String) {
	// 		debug!("{msg}");
	// 		Ok(Value::from(msg.to_string()))
	// 	}
	// ))
    // .insert(tremor_fn!(
	// 	logging|trace(_context, msg:String) {
	// 		trace!("{msg}");
	// 		Ok(Value::from(msg.to_string()))
	// 	}
	// ))
	.insert(TremorFnWrapper::new(
		"logging".to_string(),
		"info".to_string(),
		Box::new(PluggableLoggingFm::default())
	)).insert(TremorFnWrapper::new(
		"logging".to_string(),
		"warn".to_string(),
		Box::new(PluggableLoggingFm::default())
	)).insert(TremorFnWrapper::new(
		"logging".to_string(),
		"debug".to_string(),
		Box::new(PluggableLoggingFm::default())
	)).insert(TremorFnWrapper::new(
		"logging".to_string(),
		"error".to_string(),
		Box::new(PluggableLoggingFm::default())
	)).insert(TremorFnWrapper::new(
		"logging".to_string(),
		"trace".to_string(),
		Box::new(PluggableLoggingFm::default())
	));

    Ok(())
}


#[cfg(test)]
mod test {
use tremor_script::{ Registry, Object};
use tremor_value::Value;
use tremor_script::{ctx::EventContext};//, prelude::Builder};
use super::*;

	// #[test]
    // fn test_info(){
    //     let mut reg = Registry::default();
    //     let _k = load(&mut reg);
    //     let _fun = reg.find("logging", "info").unwrap();
    //     let _ctx = EventContext::new(0, None);

    //     let _res = _fun.invoke(&_ctx,&[&Value::from("info {}"),&Value::from("info")]);
    //     assert_eq!(Ok(Value::from("info info")), _res);
    // }

	#[test]
    fn test_info() {
        let mut reg = Registry::default();
        let _k = load(&mut reg);
        let _fun = reg.find("logging", "info").unwrap();
        let _ctx = EventContext::new(0, None);

		let mut obj = Object::new();
		obj.insert("key".into(), Value::from("value"));
		obj.insert("key2".into(), Value::Array(vec![Value::from("value2"), Value::from("p"), Value::from("ojn")]));

        let _res = _fun.invoke(&_ctx,&[&Value::from("info {key2} {key}"),&Value::from(obj)]);
        assert_eq!(Ok(Value::from("info [value2, p, ojn] value")), _res);
    }

	// #[test]
    // fn test_warn(){
    //     let mut reg = Registry::default();
    //     let _k = load(&mut reg);
    //     let fun = reg.find("logging", "warn").unwrap();
    //     let ctx = EventContext::new(0, None);

    //     let res = fun.invoke(&ctx, &[&Value::from("warn")]);
    //     assert_eq!(Ok(Value::from("warn")), res);
    // }

	#[test]
    fn test_warn() {
        let mut reg = Registry::default();
        let _k = load(&mut reg);
        let _fun = reg.find("logging", "warn").unwrap();
        let _ctx = EventContext::new(0, None);

		let mut obj = Object::new();
		obj.insert("key".into(), Value::from("value"));
		obj.insert("key2".into(), Value::Array(vec![Value::from("value2"), Value::from("p"), Value::from("ojn")]));

        let _res = _fun.invoke(&_ctx,&[&Value::from("info {key2} {key}"),&Value::from(obj)]);
        assert_eq!(Ok(Value::from("info [value2, p, ojn] value")), _res);
    }

	#[test]
    fn test_debug() {
        let mut reg = Registry::default();
        let _k = load(&mut reg);
        let _fun = reg.find("logging", "debug").unwrap();
        let _ctx = EventContext::new(0, None);

		let mut obj = Object::new();
		obj.insert("key".into(), Value::from("value"));
		obj.insert("key2".into(), Value::Array(vec![Value::from("value2"), Value::from("p"), Value::from("ojn")]));

        let _res = _fun.invoke(&_ctx,&[&Value::from("info {key2} {key}"),&Value::from(obj)]);
        assert_eq!(Ok(Value::from("info [value2, p, ojn] value")), _res);
    }


	#[test]
    fn test_error() {
        let mut reg = Registry::default();
        let _k = load(&mut reg);
        let _fun = reg.find("logging", "error").unwrap();
        let _ctx = EventContext::new(0, None);

		let mut obj = Object::new();
		obj.insert("key".into(), Value::from("value"));
		obj.insert("key2".into(), Value::Array(vec![Value::from("value2"), Value::from("p"), Value::from("ojn")]));

        let _res = _fun.invoke(&_ctx,&[&Value::from("info {key2} {key}"),&Value::from(obj)]);
        assert_eq!(Ok(Value::from("info [value2, p, ojn] value")), _res);
    }


	#[test]
    fn test_trace() {
        let mut reg = Registry::default();
        let _k = load(&mut reg);
        let _fun = reg.find("logging", "trace").unwrap();
        let _ctx = EventContext::new(0, None);

		let mut obj = Object::new();
		obj.insert("key".into(), Value::from("value"));
		obj.insert("key2".into(), Value::Array(vec![Value::from("value2"), Value::from("p"), Value::from("ojn")]));

        let _res = _fun.invoke(&_ctx,&[&Value::from("info {key2} {key}"),&Value::from(obj)]);
        assert_eq!(Ok(Value::from("info [value2, p, ojn] value")), _res);
    }
// 	#[test]
//     fn test_error(){
//         let mut reg = Registry::default();
//         let _k = load(&mut reg);
//         let fun = reg.find("logging", "error").unwrap();
//         let ctx = EventContext::new(0, None);

//         let res = fun.invoke(&ctx, &[&Value::from("error")]);
//         assert_eq!(Ok(Value::from("error")), res);
//     }
// 	#[test]
//     fn test_debug(){
//         let mut reg = Registry::default();
//         let _k = load(&mut reg);
//         let fun = reg.find("logging", "debug").unwrap();
//         let ctx = EventContext::new(0, None);

//         let res = fun.invoke(&ctx, &[&Value::from("debug")]);
//         assert_eq!(Ok(Value::from("debug")), res);
//     }
// 	#[test]
//     fn test_trace(){
//         let mut reg = Registry::default();
//         let _k = load(&mut reg);
//         let fun = reg.find("logging", "trace").unwrap();
//         let ctx = EventContext::new(0, None);

//         let res = fun.invoke(&ctx, &[&Value::from("trace")]);
//         assert_eq!(Ok(Value::from("trace")), res);
//     }

// 	#[test]
//     fn format() {
// 		// let mut reg = Registry::default();
//         // let _k = load(&mut reg);
// 		// let fun = reg.find("string", "info").unwrap();
// 		// dbg!(fun);
// 		// let ctx = EventContext::new(0, None);

//         // let res = fun.invoke(&ctx, &[&Value::from("trace")]);
// 		// dbg!(res.unwrap());
//         // let _f = fun("string", "info");
//         // let _v1 = Value::from("a string with {}");
//         // let _v2 = Value::from("more text");
//         // let _v3 = Value::from(123);
// 		//dbg!(f(&[&v1, &v2]).unwrap());
//         // assert_val!(f(&[&v1, &v2]), Value::from("a string with more text"));
//         // assert_val!(f(&[&v1, &v3]), Value::from("a string with 123"));
// 	}

}



