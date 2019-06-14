%% Copyright 2018-2019, Wayfair GmbH

%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at

%%      http://www.apache.org/licenses/LICENSE-2.0

%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%

-module(gen_ast).

-include_lib("pbt.hrl").

-export([gen_ast/1, fake_start/0, fake_end/0, gen_ident/0, gen_local_path/1]).

fake_start() ->
         {<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]}.

fake_end() ->
    {<<"end">>,
      [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]}.

gen_ident() ->
    [{<<"Ident">>, oneof([<<"a">>,<<"b">>,<<"c">>,<<"d">>,<<"e">>])}].


gen_local_path(_Depth) when is_number(_Depth) ->
    [{<<"exprs">>,
      [[{<<"Path">>,
         [{<<"Local">>,
           [fake_start(), fake_end(),
            {<<"segments">>,
             [gen_ident(),
              gen_ident(),
              gen_ident()]}]}]}]]}].

gen_let(Path, Expr) ->
      [{<<"Assign">>,
        [fake_start(),fake_end(),
          {<<"path">>,
           [{<<"Local">>,
             [fake_start(),fake_end(),
              {<<"segments">>,
               [[{<<"ElementSelector">>,
                  [{<<"expr">>,
                    [{<<"Literal">>,
                      [fake_start(),
                      fake_end(),
                       {<<"value">>,[{<<"Native">>,Path}]}]}]
                   },                       fake_start()
                  ,                       fake_end()
                  ]}]]}]}]},
          {<<"expr">>,
              gen_ast_inner(Expr)}]}].

gen_ast_uop(UnaryOp, Rhs) when is_float(Rhs) ->
      [{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}];
gen_ast_uop(UnaryOp, Rhs) when is_integer(Rhs) ->
      [{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}];
gen_ast_uop(UnaryOp, Rhs) when is_boolean(Rhs) ->
      [{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}].

gen_ast_bop(BinOp, Lhs, Rhs) when is_float(Lhs) and is_float(Rhs) ->
      [{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}];
gen_ast_bop(BinOp, Lhs, Rhs) when is_integer(Lhs) and is_integer(Rhs) ->
      [{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}];
gen_ast_bop(BinOp, Lhs, Rhs) when is_boolean(Lhs) and is_boolean(Rhs) ->
      [{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}];
gen_ast_bop(BinOp, Lhs, Rhs) when is_binary(Lhs) and is_binary(Rhs) ->
      [{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}].


gen_ast(X) ->
    [{<<"exprs">>, [gen_ast_inner(X)]}].

gen_ast_inner({<<"Let">>, Path, Expr}) ->
    gen_let(Path,Expr);
gen_ast_inner({BinOp, Lhs, Rhs}) ->
    gen_ast_bop(BinOp, Lhs, Rhs);
gen_ast_inner({emit, Expr}) ->
      [{<<"Emit">>, [fake_start(),fake_end(),{<<"expr">>,gen_ast_inner(Expr)}]}];
gen_ast_inner({drop, Expr}) ->
      [{<<"Drop">>, [fake_start(),fake_end(),{<<"expr">>,gen_ast_inner(Expr)}]}];
gen_ast_inner({UnaryOp, Rhs}) ->
    gen_ast_uop(UnaryOp, Rhs).
