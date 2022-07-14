# Tremor Erlang API Tests

This is home to the `tremor_api` erlang library to property test the API exposed by Tremor.

## Dependencies

* [Erlang/OTP] 
    
    Releases up to 23 are supported.

* [EQC]

    This library depends on [EQC] being available within your OTP installation.
    Download [EQC] and follow the installation instructions. A valid [EQC] license is also necessary.

* [rebar3]

    The awesome erlang build tool.
## Build

    $ rebar3 compile

## Run

To run the EQC tests defined in `tremor_api`, you first need to start a tremor server with the API enabled. This is our system to test:

    $ tremor server run config.troy

A bunch of `config.troy` files to try can be found in the `samples` directory.

Then run the tests with rebar3:

    $ rebar3 as eqc eqc

[EQC]: http://www.quviq.com/downloads/
[Erlang/OTP]: https://www.erlang.org/
[rebar3]: https://github.com/erlang/rebar3