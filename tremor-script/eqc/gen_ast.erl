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

-export([gen_ast/1, fake_start/0, fake_end/0]).

fake_start() ->
         {<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]}.

fake_end() ->
    {<<"end">>,
      [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]}.

gen_ast_uop(UnaryOp, Rhs) when is_float(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}];
gen_ast_uop(UnaryOp, Rhs) when is_integer(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}];
gen_ast_uop(UnaryOp, Rhs) when is_boolean(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Unary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,UnaryOp},
          {<<"expr">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}].

gen_ast_bop(BinOp, Lhs, Rhs) when is_float(Lhs) and is_float(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}];

gen_ast_bop(BinOp, Lhs, Rhs) when is_integer(Lhs) and is_integer(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}];
gen_ast_bop(BinOp, Lhs, Rhs) when is_boolean(Lhs) and is_boolean(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}];
gen_ast_bop(BinOp, Lhs, Rhs) when is_binary(Lhs) and is_binary(Rhs) ->
    [{<<"exprs">>,
      [[{<<"Binary">>,
         [{<<"start">>,
           [{<<"line">>,1},{<<"column">>,1},{<<"absolute">>,1}]},
          {<<"end">>,
           [{<<"line">>,1},{<<"column">>,4},{<<"absolute">>,4}]},
          {<<"kind">>,BinOp},
          {<<"lhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Lhs}]}]}]},
          {<<"rhs">>,[{<<"Literal">>,[fake_start(),fake_end(),{<<"value">>,[{<<"Native">>,Rhs}]}]}]}]}]]}].


gen_ast({BinOp, Lhs, Rhs}) ->
    gen_ast_bop(BinOp, Lhs, Rhs);
gen_ast({UnaryOp, Rhs}) ->
    gen_ast_uop(UnaryOp, Rhs).
