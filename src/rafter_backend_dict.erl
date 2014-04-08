-module(rafter_backend_dict).

-behaviour(rafter_backend).

%% rafter_backend callbacks
-export([init/1, read/2, write/2]).

-record(state, {data :: dict()}).

init(_) ->
  #state{data=dict:new()}.

read(Key, #state{data=Dict}=S) ->
  case dict:find(Key, Dict) of
    {ok, Val} ->
      {{ok, Val}, S};
    error ->
      {{error, Key}, S}
  end.

write({Key, Value}, #state{data=Dict}) ->
  {ok, #state{data=dict:store(Key, Value, Dict)}}.
