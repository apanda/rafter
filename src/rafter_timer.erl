%% -*- erlang-indent-level: 2 -*-

-module(rafter_timer).

-export([send_event_after/3, send_after/3, cancel_timer/1]).

-spec send_event_after(module(), timeout(), atom()) -> pid(). 

send_event_after(Who, When, What) ->
  io:format("Setting timer for after ~p for ~p~n", [When, Who]),
  Pid = spawn(fun() -> internal_send_event_after(Who, When, What) end),
  Pid.

internal_send_event_after(Who, When, What) ->
  receive
    cancel -> ok
    after When ->
      io:format("Timer going off after ~p for ~p~n", [When, Who]),
      gen_fsm:send_event(Who, What)
  end.

-spec send_after(timeout(), pid(), any()) -> pid(). 

send_after(When, Who, What) ->
  io:format("Setting timer for after ~p for ~p~n", [When, Who]),
  Pid = spawn(fun() -> internal_send_after(Who, When, What) end),
  {ok, Pid}.

internal_send_after(Who, When, What) ->
  receive
    cancel -> ok
    after When ->
      io:format("Timer going off after ~p for ~p~n", [When, Who]),
      Who ! What
  end.

-spec cancel_timer(pid()) -> ok.

cancel_timer(Pid) -> 
  Pid ! cancel,
  {ok, cancel}.
