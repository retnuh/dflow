%%%-------------------------------------------------------------------
%%% @author Hunter Kelly <retnuh@gmail.com>
%%% @copyright (C) 2011, Hunter Kelly
%%% @doc
%%% Interface to stored dflow results.
%%% @end
%%% Created :  1 Jul 2011 by Hunter Kelly <retnuh@gmail.com>
%%%-------------------------------------------------------------------
-module(dfq).
-include("dflow.hrl").

-include_lib("stdlib/include/qlc.hrl").
%% TODO wrap with test macros
-include_lib("eunit/include/eunit.hrl").


%% API
-export([completed_data/1, completed/1, all_data/1, all/1, exists/2,
         delete_matching_data/2, filter_data/2]).

%%%===================================================================
%%% API
%%%===================================================================

exists(Data, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    UUID = dflow:uuid(Stage, Module, Data),
    case mnesia:dirty_read(Table, UUID) of
        [] -> false;
        _ -> true
    end.

delete_matching_data(Predicate, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    Q = qlc:q([ X || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module, X#dflow.status =:= complete,
                     Predicate(X#dflow.data)]),
    fold(fun(D, Acc) -> mnesia:delete(Table, D#dflow.uuid, write), [D|Acc] end, [], Q).

filter_data(Predicate, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    Q = qlc:q([ X#dflow.data || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module, X#dflow.status =:= complete,
                     Predicate(X#dflow.data)]),
    do(Q).


    
completed_data({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do(qlc:q([ X#dflow.data || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                    X#dflow.module =:= Module, X#dflow.status =:= complete
       ])).  

completed({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do(qlc:q([ X || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module, X#dflow.status =:= complete
       ])).  

all_data({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do(qlc:q([ X#dflow.data || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                    X#dflow.module =:= Module
       ])).  

all({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do(qlc:q([ X || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module
       ])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

fold(P, Acc0, Q) ->
    {atomic, Val} = mnesia:transaction(fun() -> qlc:fold(P, Acc0, Q) end),
    Val.

do(Q) ->
    {atomic, Val} = mnesia:transaction(fun() -> qlc:e(Q) end),
    Val.
