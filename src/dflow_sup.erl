-module(dflow_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

%% TODO: These timeout values should be paramaterized, in app or something.
%% Same for 5000 in CHILD Macro (shutdown)
init([]) ->
    %% io:format("Starting dflow_sup"),
    WorkerSup = ?CHILD(dflow_worker_sup, supervisor),
    DFlowServer = ?CHILD(dflow, worker),
    {ok, { {rest_for_one, 5, 10}, [WorkerSup, DFlowServer]} }.

