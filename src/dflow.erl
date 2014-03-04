%%%-------------------------------------------------------------------
%%% @author Hunter Kelly <retnuh@gmail.com>
%%% @copyright (C) 2011, Hunter Kelly
%%% @doc
%%%
%%% @end
%%% Created : 29 Jun 2011 by Hunter Kelly <retnuh@gmail.com>
%%%-------------------------------------------------------------------
-module(dflow).
-include("dflow.hrl").
-include_lib("stdlib/include/qlc.hrl").
-behaviour(gen_server).

%% API
-export([start_link/0, add_data/2, add_datum/2, register/1, return_result/2, stop/0]).
-export([uuid/3, sync/0, add_raw/1, recompute_uuid/2, unregister/1]).

%% gen_server callbacks
-export([init/1, handle_cast/2, handle_info/2,  handle_call/3,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {workers}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], [
                                                         % {debug, [trace]}
                                                         ]).

stop() ->
    gen_server:cast(?SERVER, stop).

sync() ->
    gen_server:call(?SERVER, sync).

%%--------------------------------------------------------------------
%% @doc
%% Add a single piece of data to the dflow
%%
%% @spec add_datum({Stage, DFlowModule}, Datum) -> ok
%% @end
%%--------------------------------------------------------------------
add_datum({_Stage, _DFlowModule}=DFlow, Datum) ->
    gen_server:call(?SERVER, {add_datum, DFlow, Datum}).

%%--------------------------------------------------------------------
%% @doc
%% Add a list of data to the dflow
%%
%% @spec add_datum({Stage, DFlowModule}, Data) -> ok
%% @end
%%--------------------------------------------------------------------
add_data({_Stage, _DFlowModule}=DFlow, Data) when is_list(Data) ->
    gen_server:cast(?SERVER, {add_data, DFlow, Data}).

add_raw(Data) when is_record(Data, dflow) ->
    gen_server:cast(?SERVER, {add_raw, Data}).

%%--------------------------------------------------------------------
%% @doc
%% Add a list of data to the dflow
%%
%% @spec add_datum({Stage, DFlowModule}, Data) -> ok
%% @end
%%--------------------------------------------------------------------
recompute_uuid({_Stage, _DFlowModule}=DFlow, UUID) when is_list(UUID) ->
    gen_server:cast(?SERVER, {recompute, DFlow, UUID}).

%%--------------------------------------------------------------------
%% @doc
%% Return the results of a computation and push it through the DFlow.
%%
%% @spec return_result(DFlow, Result) -> ok
%% @end
%%--------------------------------------------------------------------
return_result(DFlow, Result) ->
    gen_server:cast(?SERVER, {result, DFlow, Result}).


register(X) ->
    Ret = register_tables(X, []),
    restart_incomplete(X),
    Ret.

restart_incomplete([]) ->
    ok;
restart_incomplete([Table | Rest]) ->
    Txn = fun() ->
                  Q = qlc:q([ X || X <- mnesia:table(Table), X#dflow.status =:= created]),
                  qlc:fold(fun(R, _) -> compute_stage(R) end, ok, Q)
          end,
    %% TODO spawn this? any point?
    mnesia:transaction(Txn),
    restart_incomplete(Rest).
                               

register_tables([], Acc) ->
    lists:reverse(Acc);
register_tables([{Table, Opts} | Rest], Acc) when is_list(Opts) ->
    Res = register_table(Table, Opts),
    register_tables(Rest, [Res | Acc]);
register_tables([Table | Rest], Acc) when is_atom(Table) ->
    Res = register_table(Table, []),
    register_tables(Rest, [Res|Acc]).

% TODO potential race cond between table being in ready state and not
% recognized as created?
register_table(Table, Opts) ->
    try mnesia:table_info(Table, attributes) of
        _Attrs -> mnesia:wait_for_tables([Table], infinity),
        io:format("Dflow: found table ~p with attrs: ~p~n",[Table,_Attrs]),
        exists                                    
    catch
        exit:{aborted,{no_exists,Table,attributes}} ->
            io:format("Dflow: table appears not to exist, creating: ~p ~p~n",
                      [Table,  [ {attributes, record_info(fields, dflow)},
                                         {record_name, dflow} | Opts]]),                                               
            _Ret = mnesia:create_table(Table, [ {attributes, record_info(fields, dflow)},
                                         {record_name, dflow} | Opts]),
            io:format("Dflow: create_table for ~p returned ~p~n",[Table,_Ret]),
            created
    end.

unregister(Table) ->
    %% Anything else to do?
    {atomic, ok} = mnesia:delete_table(Table).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{workers=gb_trees:empty()}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(sync, _From, State) ->
    {reply, ok, State};
handle_call({add_datum, DFlow, Datum}, _From, State) ->
    [Res] = run_transactions([inject_datum(DFlow, Datum)]),
    {reply, Res, State}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(Info, State) ->
    error_logger:warning_msg("Unknown message, ignored: ~p", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({add_data, DFlow, Data}, State) when is_list(Data) ->
    inject(DFlow, Data, []),
    {noreply, State};
handle_cast({add_raw, #dflow{stage=Stage,module=DFlowMod}=DFlow}, State) ->
    Table = DFlowMod:table_for_stage(Stage),
    {atomic, _} = mnesia:transaction(fun()-> mnesia:write(Table, DFlow, write) end),
    {noreply, State};
handle_cast({recompute, Dflow, UUID}, State) ->
    recompute(Dflow, UUID),
    {noreply, State};
handle_cast({result, DFlow, Result}, State) ->
    next_stage(DFlow, Result),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, "Stop received", State}.

%%%===================================================================
%%% Internal functions  
%%%===================================================================
inject_datum({Stage, DFlowMod}, Datum) ->
    UUID = uuid(Stage, DFlowMod, Datum),
    Rec = #dflow{uuid=UUID,stage=Stage,module=DFlowMod,data=Datum,created=now(),status=created},
    Table = DFlowMod:table_for_stage(Stage),
    Txn = fun() ->
                  case mnesia:dirty_read(Table, UUID) of
                      [] -> mnesia:write(Table, Rec, write),
                            {inserted, Rec};
                      [X] -> {exists, X}
                  end
          end,
    PostCommit = fun() -> compute_stage(Rec) end,
    { Txn, PostCommit }.

recompute(DFlow, UUID) ->
    Rec = dfq:uuid_dflow(UUID, DFlow),
    compute_stage(Rec).

run_transactions(ReversedTxns) ->
    Txns = lists:reverse(ReversedTxns),
    Txn = fun() -> lists:map(fun({T, _}) -> T() end, Txns) end,
    {atomic, Results} = mnesia:transaction(Txn),
    lists:zipwith(fun({_,P}, {inserted, _}=T) when is_function(P) -> P(), T;
                     ({_, _}, R) -> {exists, R}
                  end, Txns, Results).

inject(_, [], ReversedTxns) ->
    run_transactions(ReversedTxns);
inject({_Stage, _Mod}=DFlow, [Datum | Rest], Txns) ->
    Funs = inject_datum(DFlow, Datum),
    inject(DFlow, Rest, [Funs|Txns]).

compute_stage(#dflow{stage=Stage,module=DFlowMod}=DFlow) ->
    Funs = DFlowMod:functions_for_stage(Stage),
    compute_stage(DFlow, Funs).
compute_stage(#dflow{}=DFlow, identity) ->
    next_stage(DFlow, DFlow#dflow.data);
compute_stage(DFlow, none) ->
    complete_stage(DFlow, []);
compute_stage(#dflow{}=DFlow, Funs) when is_list(Funs) ->
    lists:foreach(fun(FunInfo) -> run_function_on_datum(DFlow, FunInfo) end, Funs).

run_function_on_datum(DFlow, {Mod, Fun, XArgs}) ->
    dflow_worker:start(Mod, Fun, DFlow, XArgs).

next_stage(#dflow{stage=CurStage,module=DFlowMod}=DFlow, Result) ->
    Pairs = DFlowMod:next_stage(CurStage, Result),
    InjectTxns = lists:map(fun(P) -> inject_result(DFlowMod, P) end, Pairs),
    complete_stage(DFlow, InjectTxns).


complete_stage(#dflow{stage=CurStage,module=DFlowMod}=DFlow, Txns) ->
    UpdateTxn = fun() ->
                        D = completed_dflow(DFlow, DFlowMod:is_stage_transient(CurStage)),
                        mnesia:write(DFlowMod:table_for_stage(CurStage), D, write)
                end,
    run_transactions([{UpdateTxn, ok} | Txns]).

completed_dflow(DFlow, true) ->
    %% Should this be the empty string instead of transient?
    DFlow#dflow{completed=now(),status=complete,data=transient};
completed_dflow(DFlow, false) ->
    DFlow#dflow{completed=now(),status=complete}.


inject_result(_CurDFlowMod, {{_Stage, _DFlowMod}=DFlow, Datum}) ->
    inject_datum(DFlow, Datum);
inject_result(CurDFlowMod, {Stage, Datum}) when is_atom(Stage) ->
    inject_datum({Stage, CurDFlowMod}, Datum).

uuid(Stage, DFlowMod, Data) ->
    Name = "dflow-"++atom_to_list(Stage)++ "-" ++atom_to_list(DFlowMod),
    uuid:to_string(uuid:sha(list_to_binary(Name), term_to_binary(Data))).
