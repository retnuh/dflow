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
         delete_matching_data/2, filter_data/2, delete/2, first/1,
         delete_uuid/2, delete_incomplete/2, uuid_data/2, uuid_dflow/2]).

%%%===================================================================
%%% API
%%%===================================================================

first({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    Rec = do_async_dirty(
            fun() ->
                    QH = qlc:q([ X || X <- mnesia:table(Table),
                                      X#dflow.stage =:= Stage,
                                      X#dflow.module =:= Module]),
                    QC = qlc:cursor(QH),
                    Val = qlc:next_answers(QC, 1),
                    qlc:delete_cursor(QC),
                    Val
            end),
    case Rec of
        [] -> none;
        [X] -> {ok, X#dflow.data}
    end.

exists(Data, {Stage, Module}) when not is_record(Data, dflow) ->
    Table = Module:table_for_stage(Stage),
    UUID = dflow:uuid(Stage, Module, Data),
    case mnesia:dirty_read(Table, UUID) of
        [] -> false;
        _ -> true
    end.

uuid_data(UUID, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    [X | _] = mnesia:dirty_read(Table, UUID),
    X#dflow.data.

uuid_dflow(UUID, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    [X | _] = mnesia:dirty_read(Table, UUID),
    X.


delete_incomplete(Module, Table) ->
    Q = qlc:q([ X || X <- mnesia:table(Table), X#dflow.module =:= Module, X#dflow.status =/= complete]),
    fold(fun(D, Acc) -> mnesia:delete(Table, D#dflow.uuid, write), [D|Acc] end, [], Q).
    
delete_uuid(UUID, {Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do(fun() -> mnesia:delete(Table, UUID, write) end).
               
delete(Data, {Stage, Module}) when not is_record(Data, dflow) ->
    Table = Module:table_for_stage(Stage),
    UUID = dflow:uuid(Stage, Module, Data),
    do(fun() ->
               case mnesia:dirty_read(Table, UUID) of
                   [] -> false;
                   [X] -> mnesia:delete(Table, UUID, write), X
               end
       end).

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
    do_q(Q).


    
completed_data({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do_q(qlc:q([ X#dflow.data || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                    X#dflow.module =:= Module, X#dflow.status =:= complete
       ])).  

completed({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do_q(qlc:q([ X || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module, X#dflow.status =:= complete
       ])).  

all_data({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do_q(qlc:q([ X#dflow.data || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                    X#dflow.module =:= Module
       ])).  

all({Stage, Module}) ->
    Table = Module:table_for_stage(Stage),
    do_q(qlc:q([ X || X <- mnesia:table(Table), X#dflow.stage =:= Stage,
                     X#dflow.module =:= Module
       ])).

%%%===================================================================
%%% Internal functions
%%%===================================================================

fold(P, Acc0, Q) ->
    {atomic, Val} = mnesia:transaction(fun() -> qlc:fold(P, Acc0, Q) end),
    Val.

do(F) when is_function(F) ->
    {atomic, Val} = mnesia:transaction(F),
    Val.

do_async_dirty(F) when is_function(F) ->
    mnesia:async_dirty(F).

do_q(Q) ->
    {atomic, Val} = mnesia:transaction(fun() -> qlc:e(Q) end),
    Val.
