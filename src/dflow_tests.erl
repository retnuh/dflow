-module(dflow_tests).
-behaviour(dataflow).
-export([functions_for_stage/1, next_stage/2, prepend/2, flaky/1]).
-include_lib("eunit/include/eunit.hrl").

%%% dataflow API

functions_for_stage(flaky) ->
    [{?MODULE, flaky, []}];
functions_for_stage(foo) ->                     % Demonstrate that a stage can have multiple functions
    [{?MODULE, prepend, ["foo1"]}, {?MODULE, prepend, ["foo2"]}];
functions_for_stage(bar) ->
    [{?MODULE, prepend, ["bar"]}];
functions_for_stage(baz) ->
    [{?MODULE, prepend, ["baz"]}].

next_stage(flaky, Result) ->
    [{foo, Result}];
next_stage(foo, Result) ->
    [{bar, Result}];
next_stage(bar, Result) ->
    [{baz, Result}, {baz, "extra-" ++ Result}]; % results can produce multiple new functions
next_stage(baz, Result) ->
    %% ?debugVal(Result),
    ?MODULE ! Result,
    [].

%%% Functions that actually "do stuff"

prepend(Data, Prefix) ->
    %%    ?debugFmt("Prefix: ~p-~p", [Prefix, Data]),
    Prefix ++ "-" ++ Data.

flaky(Data) ->
    {A1,A2,A3} = now(),
    random:seed(A1, A2, A3),
    case random:uniform(2) of
        1 -> error("Faux failure");
        2 -> Data
    end.
            
%%% Test related functions

setUp() ->
    % application:start(dflow),
    ok.

tearDown(_) ->
    case whereis(?MODULE) of
        Pid when is_pid(Pid) -> unregister(?MODULE);
        undefined -> ok
    end,
    % application:stop(dflow),
    ok.

test_receive(0) ->
    ok;
test_receive(N) ->
    %% ?debugFmt("~p Waiting to receive ~p more items", [self(), N]),
    receive
        Val -> X = match_result(N, Val), test_receive(X)
    after 2000 ->
            error("Timeout")
    end.

match_result(N, "-item" ++  [X]) when X >= $1, X=<$9 ->
    N-1;
match_result(N, "baz-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "extra-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "bar-" ++ Rest) ->
    match_result(N, Rest);
match_result(N, "foo" ++ [X | Rest]) when X >= $1, X=<$2 ->
    match_result(N, Rest).

basic_test_() ->
    {setup, fun setUp/0, fun tearDown/1, [
     fun() ->
             register(?MODULE, self()),
             %% ?debugFmt("Registered ~p on ~p", [?MODULE, self()]),
             dflow:add_datum({foo, ?MODULE}, "item1"),
             dflow:add_data({foo, ?MODULE}, ["item2", "item3"]),
             test_receive(12)
     end
    ]}.

flaky_test_() ->
    {setup, fun setUp/0, fun tearDown/1, [
     fun() ->
             register(?MODULE, self()),
             %% ?debugFmt("Registered ~p on ~p", [?MODULE, self()]),
             dflow:add_datum({flaky, ?MODULE}, "item4"),
             test_receive(4)
     end
    ]}.

%% nth_primes_test_() ->
%%     {setup, fun start/0, fun(_) -> stop() end,
%%      [
%%       ?_assertEqual(2, nth_prime(1)),
%%       ?_assertEqual(3, nth_prime(2)),
%%       ?_assertEqual(17, nth_prime(7)),
%%       ?_assertEqual(31, nth_prime(11))
%%      ]}.

%% next_primes_test_() ->
%%     {setup, fun start/0, fun(_) -> stop() end,
%%      {generator, ?MODULE, next_primes_test_generator}
%%     }.

%% next_primes_test_generator() ->
%%     next_primes_test_generator(first_prime(), [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]).
%% next_primes_test_generator(_, []) ->
%%     [];
%% next_primes_test_generator({P, T}, [E | Rest]) ->
%%     {generator, fun() ->
%%                         [?_assertEqual(E, P),
%%                          next_primes_test_generator(next_prime(T), Rest)]
%%                 end
%%     }.
