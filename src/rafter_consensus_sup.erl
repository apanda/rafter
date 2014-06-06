-module(rafter_consensus_sup).

-behaviour(supervisor).

-include("rafter_opts.hrl").

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

start_link(Me, Opts) when is_atom(Me) ->
    SupName = name(Me, "sup"),
    start_link(Me, SupName, Me, Opts);
start_link(Me, Opts) ->
    {Name, _Node} = Me,
    SupName = name(Name, "sup"),
    start_link(Name, SupName, Me, Opts).

init([NameAtom, Me, #rafter_opts{log_service = Log} = Opts]) ->
    io:format("Starting ~p~n", [NameAtom]),
    LogServer = { rafter_log,
                 {Log, start_link, [NameAtom, Opts]},
                 permanent, 5000, worker, [Log]},
    ConsensusFsm = { rafter_consensus_fsm,
                    {rafter_consensus_fsm, start_link, [NameAtom, Me, Opts]},
                    permanent, 5000, worker, [rafter_consensus_fsm]},
    TimeServer = { rafter_timer_fsm,
                   {rafter_timer_fsm, start_link, [Me]},
                   permanent, 5000, worker, [rafter_timer_fsm]},
    {ok, {{one_for_all, 5, 10}, [LogServer, TimeServer, ConsensusFsm]}}.

%% ===================================================================
%% Private Functions
%% ===================================================================
name(Name, Extension) ->
    list_to_atom(atom_to_list(Name) ++ "_" ++ Extension).

start_link(NameAtom, SupName, Me, Opts) ->
    supervisor:start_link({local, SupName}, ?MODULE, [NameAtom, Me, Opts]).
