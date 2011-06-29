%%%-------------------------------------------------------------------
%%% @author Hunter Kelly <retnuh@gmail.com>
%%% @copyright (C) 2011, Hunter Kelly
%%% @doc
%%%
%%% @end
%%% Created : 29 Jun 2011 by Hunter Kelly <retnuh@gmail.com>
%%%-------------------------------------------------------------------
-module(dflow).

-behaviour(gen_server).

%% API
-export([start_link/0, add_data/2, add_datum/2, return_result/2, stop/0]).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], [{debug, [trace]}]).

stop() ->
    gen_server:cast(?SERVER, stop).

%%--------------------------------------------------------------------
%% @doc
%% Add a single piece of data to the dflow
%%
%% @spec add_datum({Stage, DFlowModule}, Datum) -> ok
%% @end
%%--------------------------------------------------------------------
add_datum({_Stage, _DFlowModule}=DFlow, Datum) ->
    gen_server:cast(?SERVER, {add_datum, DFlow, Datum}).

%%--------------------------------------------------------------------
%% @doc
%% Add a list of data to the dflow
%%
%% @spec add_datum({Stage, DFlowModule}, Data) -> ok
%% @end
%%--------------------------------------------------------------------
add_data({_Stage, _DFlowModule}=DFlow, Data) when is_list(Data) ->
    gen_server:cast(?SERVER, {add_data, DFlow, Data}).

%%--------------------------------------------------------------------
%% @doc
%% Return the results of a computation and push it through the DFlow.
%%
%% @spec return_result(DFlow, Result) -> ok
%% @end
%%--------------------------------------------------------------------
return_result(DFlow, Result) ->
    gen_server:cast(?SERVER, {result, DFlow, Result}).

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
    io:format("Starting dflow~n"),    
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
handle_call(unused, _From, State) ->
     {noreply, State}.


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
handle_cast({add_datum, DFlow, Datum}, State) ->
    inject_datum(DFlow, Datum),
    {noreply, State};
handle_cast({add_data, DFlow, Data}, State) when is_list(Data) ->
    inject(DFlow, Data),
    {noreply, State};
handle_cast({result, DFlow, Result}, State) ->
    next_stage(DFlow, Result),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, "Stop received", State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

inject_datum({Stage, DFlowMod}=DFlow, Datum) ->
    % create initial record
    Funs = DFlowMod:functions_for_stage(Stage),
    lists:foreach(fun(FunInfo) -> run_function_on_datum(DFlow, Datum, FunInfo) end, Funs).

inject(_, []) ->
    ok;
inject({_Stage, _Mod}=DFlow, [Datum | Rest]) ->
    inject_datum(DFlow, Datum),
    inject(DFlow, Rest).

run_function_on_datum(DFlow, Datum, {Mod, Fun, XArgs}) ->
    dflow_worker:start(Mod, Fun, DFlow, [Datum | XArgs]).

next_stage({CurStage, DFlowMod}, Result) ->
    %% save results, but after inject_results has been called (so if system crashes
    %% before all new ones are recorded, etc.  Maybe created, computed, completed as
    %% saved states?
    Pairs = DFlowMod:next_stage(CurStage, Result),
    lists:foreach(fun(P) -> inject_result(DFlowMod, P) end, Pairs).

inject_result(_CurDFlowMod, {{_Stage, _DFlowMod}=DFlow, Datum}) ->
    inject_datum(DFlow, Datum);
inject_result(CurDFlowMod, {Stage, Datum}) when is_atom(Stage) ->
    inject_datum({Stage, CurDFlowMod}, Datum).

