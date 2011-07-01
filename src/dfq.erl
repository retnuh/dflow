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

%% API
-export([completed_data/1, completed/1]).

%%%===================================================================
%%% API
%%%===================================================================

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

%%%===================================================================
%%% Internal functions
%%%===================================================================

do(Q) ->
    {atomic, Val} = mnesia:transaction(fun() -> qlc:e(Q) end),
    Val.
