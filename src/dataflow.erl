-module(dataflow).

-export([behaviour_info/1]).

behaviour_info(callbacks) ->
    [{functions_for_stage,1},
     {next_stage, 2},
     {table_for_stage,1},
     {is_stage_transient, 1}];
behaviour_info(_Other) ->
    undefined.
