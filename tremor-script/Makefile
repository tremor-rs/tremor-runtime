run-eqc: compile 
	LD_LIBRARY_PATH="../target/debug:$LD_LIBRARY_PATH" rebar3 as eqc eqc -n 1000

simple:	compile
	LD_LIBRARY_PATH="../target/debug:$LD_LIBRARY_PATH" rebar3 as eqc eqc --properties prop_simple_expr -n 1000

compile:
	cargo build -p tremor-script --features erlang-float-testing

clean:
	make -C c_src clean
	rm -rf _build
	rm -r priv 