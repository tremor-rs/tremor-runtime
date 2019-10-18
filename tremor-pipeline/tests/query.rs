use tremor_pipeline::errors::*;
use tremor_pipeline::query::Query;

fn to_pipe(query: &str) -> Result<()> {
    let reg = tremor_script::registry();
    let aggr_reg = tremor_script::aggr_registry();
    let q = Query::parse(query, &reg, &aggr_reg)?;
    q.to_pipe()?;
    Ok(())
}

macro_rules! test_files {

    ($($file:ident),*) => {
        $(
            #[test]
            fn $file() -> Result<()> {
                let contents = include_bytes!(concat!("queries/", stringify!($file), ".trickle"));
                to_pipe(std::str::from_utf8(contents)?)
            }
        )*
    };
}

test_files!(for_in_select, script_with_args);
