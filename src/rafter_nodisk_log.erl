-module(rafter_nodisk_log).

-behaviour(gen_server).

-include_lib("kernel/include/file.hrl").

-include("rafter.hrl").
-include("rafter_opts.hrl").

%% API
-export([start_link/2, stop/1, append/2, check_and_append/3, get_last_entry/1, get_entry/2, get_term/2,
        get_last_index/1, get_last_term/1, get_config/1, set_metadata/3,
        get_metadata/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3, format_status/2]).

%%=============================================================================
%% Logfile Structure
%%=============================================================================
%% @doc A log is made up of a file header and entries. The header contains file
%%      metadata and is written once at file creation. Each entry is a binary
%%      of arbitrary size containing header information and is followed by a trailer.
%%      The formats of the file header and entries are described below.
%%
%%         File Header Format
%%         -----------------------
%%         <<Version:8>>
%%
%%         Entry Format
%%         ----------------
%%         <<Sha1:20/binary, Type:8, Term:64, Index: 64, DataSize:32, Data/binary>>
%%
%%         Sha1 - hash of the rest of the entry,
%%         Type - ?CONFIG | ?OP
%%         Term - The term of the entry
%%         Index - The log index of the entry
%%         DataSize - The size of Data in bytes
%%         Data - Data encoded with term_to_binary/1
%%
%%     After each log entry a trailer is written. The trailer is used for
%%     detecting incomplete/corrupted writes, pointing to the latest config and
%%     traversing the log file backwards.
%%
%%         Trailer Format
%%         ----------------
%%         <<Crc:32, ConfigStart:64, EntryStart:64, ?MAGIC:64>>
%%
%%         Crc - checksum, computed with erlang:crc32/1, of the rest of the trailer
%%         ConfigStart - file location of last seen config,
%%         EntryStart - file location of the start of this entry
%%         ?MAGIC - magic number marking the end of the trailer.
%%                  A fully consistent log should always have
%%                  the following magic number as the last 8 bytes:
%%                  <<"\xFE\xED\xFE\xED\xFE\xED\xFE\xED">>
%%

-define(MAX_HINTS, 1000).

-type index() :: non_neg_integer().
-type offset() :: non_neg_integer().

-record(state, {
    version :: non_neg_integer(),
    write_location = 0 :: non_neg_integer(),
    config :: #config{},
    config_loc :: offset(),
    meta :: #meta{},
    last_entry :: #rafter_entry{},
    index = 0 :: index(),
    term = 0 :: non_neg_integer(),
    data :: ets:tid(),
    %hints :: ets:tid(),
    %hint_prunes = 0 :: non_neg_integer(),

    %% frequency of number of entries scanned in get_entry/2 calls
    seek_counts = dict:new()}).

-define(MAGIC, <<"\xFE\xED\xFE\xED\xFE\xED\xFE\xED">>).
-define(MAGIC_SIZE, 8).
-define(HEADER_SIZE, 41).
-define(TRAILER_SIZE, 28).
-define(FILE_HEADER_SIZE, 1).
-define(READ_BLOCK_SIZE, 1048576). %% 1MB
-define(LATEST_VERSION, 1).

%% Entry Types
-define(NOOP, 0).
-define(CONFIG, 1).
-define(OP, 2).
-define(ALL, [?CONFIG, ?OP]).

-define(ETS_OPTS, [ordered_set, protected]).

%%====================================================================
%% API
start_link(Peer, Opts) ->
    gen_server:start_link({local, logname(Peer)}, ?MODULE, [Peer, Opts], []).

stop(Peer) ->
    gen_server:cast(logname(Peer), stop).

%% @doc check_and_append/3 gets called in the follower state only and will only
%% truncate the log if entries don't match. It never truncates and re-writes
%% committed entries as this violates the safety of the RAFT protocol.
check_and_append(Peer, Entries, Index) ->
    gen_server:call(logname(Peer), {check_and_append, Entries, Index}).

%% @doc append/2 gets called in the leader state only, and assumes a
%% truncated log.
append(Peer, Entries) ->
    %io:format("About to call append~n"),
    R = gen_server:call(logname(Peer), {append, Entries}),
    %io:format("Done with append ~p~n", [R]),
    R.

get_config(Peer) ->
    gen_server:call(logname(Peer), get_config).

get_last_index(Peer) ->
    gen_server:call(logname(Peer), get_last_index).

get_last_entry(Peer) ->
    gen_server:call(logname(Peer), get_last_entry).

get_last_term(Peer) ->
    case get_last_entry(Peer) of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

get_metadata(Peer) ->
    gen_server:call(logname(Peer), get_metadata).

set_metadata(Peer, VotedFor, Term) ->
    gen_server:call(logname(Peer), {set_metadata, VotedFor, Term}).

get_entry(Peer, Index) ->
    %io:format("Call to get_entry~n"),
    gen_server:call(logname(Peer), {get_entry, Index}).

get_term(Peer, Index) ->
    %io:format("Call to get_term~n"),
    case get_entry(Peer, Index) of
        {ok, #rafter_entry{term=Term}} ->
            Term;
        {ok, not_found} ->
            0
    end.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Name, _Opts]) ->
    %LogName = Logdir++"/rafter_"++atom_to_list(Name)++".log",
    %MetaName = Logdir++"/rafter_"++atom_to_list(Name)++".meta",
    %case Clean of
      %true -> file:delete(LogName),
              %file:delete(MetaName);
      %false -> ok
    %end,
    %{ok, LogFile} = file:open(LogName, [append, read, binary, raw]),
    %{ok, #file_info{size=Size}} = file:read_file_info(LogName),
    {ok, Meta} = read_metadata(),
    {ConfigLoc, Config, Term, Index, WriteLocation, Version} = init_file(),
    LastEntry = undefined,
    DataTable = list_to_atom("rafter_data_" ++ atom_to_list(Name)),
    {ok, #state{version=Version,
                write_location=WriteLocation,
                term=Term,
                index=Index,
                meta=Meta,
                config=Config,
                config_loc = ConfigLoc,
                last_entry=LastEntry,
                data=ets:new(DataTable, ?ETS_OPTS)}}.

format_status(_, [_, State]) ->
    %Data = lager:pr(State, ?MODULE),
    Data = io_lib:format("~p", [State]),
    [{data, [{"StateData", Data}]}].

handle_call({append, Entries}, _From, #state{data=Data}=State) ->
    %io:format("Called append for ~p~n", [Entries]),
    NewState = write_entries(Data, Entries, State),
    %io:format("Done writing for ~p~n", [Entries]),
    Index = NewState#state.index,
    %io:format("Written ~p returning ~n", [Entries]),
    {reply, {ok, Index}, NewState};

handle_call(get_config, _From, #state{config=Config}=State) ->
    {reply, Config, State};

handle_call(get_last_entry, _From, #state{last_entry=undefined}=State) ->
    {reply, {ok, not_found}, State};
handle_call(get_last_entry, _From, #state{last_entry=LastEntry}=State) ->
    {reply, {ok, LastEntry}, State};

handle_call(get_last_index, _From, #state{index=Index}=State) ->
    {reply, Index, State};

handle_call(get_metadata, _, #state{meta=Meta}=State) ->
    {reply, Meta, State};

handle_call({set_metadata, VotedFor, Term}, _, S) ->
    Meta = #meta{voted_for=VotedFor, term=Term},
    {reply, ok, S#state{meta=Meta}};

handle_call({check_and_append, Entries, Index}, _From, #state{data=Data}=S) ->
    %Loc0 = closest_forward_offset(Hints, Index),
    {Loc, Count} = get_pos(Data, Index),
    State = update_counters(Count, S),
    #state{index=NewIndex}=NewState = maybe_append(Index, Loc, Entries, State),
    {reply, {ok, NewIndex}, NewState};

handle_call({get_entry, Index}, _From, #state{data=Data}=State0) ->
    %io:format("Handling call for get_entry~n"),
    X = find_entry(Data, Index),
    %io:format("In get_entry, ran find_entry got ~p~n", [X]),
    {Res, NewState} = 
    case  X of
        {not_found, Count} -> 
            State = update_counters(Count, State0),
            {not_found, State};
        {Entry, _NextLoc, Count} ->
            State = update_counters(Count, State0),
            {Entry, State}
    end,
    {reply, {ok, Res}, NewState}.

-spec update_counters(offset(), #state{}) -> #state{}.
update_counters(Distance, #state{seek_counts=Dict0}
                                                   =State) ->
    Dict = dict:update_counter(Distance, 1, Dict0),
    State#state{seek_counts=Dict}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% Internal Functions
%%====================================================================

maybe_append(_, _, [], State) ->
    State;
maybe_append(Index, eof, [Entry | Entries], State) ->
    NewState = write_entry(Entry, State),
    maybe_append(Index+1, eof, Entries, NewState);
maybe_append(Index, Loc, [#rafter_entry{term=Term}=Entry | Entries],
             State=#state{data=DataF}) ->
    case read_entry(DataF, Loc) of
        {entry, Data, NewLocation} ->
            %io:format("Read entry ~p with index ~p, term ~p looking for ~p ~p~n", [Data, Idx, T, Index, Term]),
            case Data of
                #rafter_entry{index=Index, term=Term} ->
                    maybe_append(Index+1, NewLocation, Entries, State);
                #rafter_entry{index=Index, term=_} ->
                    ok = truncate(DataF, Loc),
                    State1 = State#state{write_location=Loc},
                    State2 = write_entry(Entry, State1),
                    maybe_append(Index + 1, eof, Entries, State2)
            end;
        eof ->
            ok = truncate(DataF, Loc),
            State1 = State#state{write_location=Loc},
            State2 = write_entry(Entry, State1),
            maybe_append(Index+1, eof, Entries, State2)
    end.

logname({Name, _Node}) ->
    list_to_atom(atom_to_list(Name) ++ "_log");
logname(Me) ->
    list_to_atom(atom_to_list(Me) ++ "_log").

init_file() ->
    {0, #config{}, 0, 0, 0, ?LATEST_VERSION}.

write_entries(_Data, Entries, State) ->
    NewState = lists:foldl(fun write_entry/2, State, Entries),
    NewState.

write_entry(#rafter_entry{type=Type, cmd=Cmd}=Entry, S=#state{write_location=Loc,
                                                              config=Config,
                                                              config_loc=ConfigLoc,
                                                              index=Index,
                                                              data=Data}) ->
    %io:format("In write entry~n"),
    NewIndex = Index + 1,
    NewEntry = Entry#rafter_entry{index=NewIndex},
    {NewConfigLoc, NewConfig} =
    case Type of
        config ->
            {Loc, Cmd};
        _ ->
            {ConfigLoc, Config}
    end,
    %ok = file:write(File, <<BinEntry/binary, Trailer/binary>>),
    NewLoc = Loc + 1,
    true = ets:insert(Data, {NewLoc, NewEntry}),
    S#state{index=NewIndex,
            config=NewConfig,
            write_location=NewLoc,
            config_loc=NewConfigLoc,
            last_entry=NewEntry}.


%% TODO: Write to a tmp file then rename so the write is always atomic and the
%% metadata file cannot become partially written.
read_metadata() ->
    {ok, #meta{}}.

truncate(Data, Pos) ->
    case ets:lookup(Data, Pos) of 
        [] -> ok;
        _ -> ets:delete(Data, Pos),
             truncate(Data, Pos + 1)
    end.

get_pos(Data, Index) ->
    get_pos(Data,  Index, 0).

get_pos(Data, Index, Count) ->
    case ets:lookup(Data, Index) of
      [] ->
         {eof, Count};
      _ ->
        {Index, Count}
    end.

%% @doc Find an entry at the given index in a file. Search forward from Loc.
find_entry(Data, Index) ->
    %io:format("Calling find_entry~n"),
    find_entry(Data, Index, 0).
find_entry(DataF, Index, Count) ->
    Ret = ets:lookup(DataF, Index),
    case Ret of
      [] -> 
         {not_found, Count};
      [{Index, Data}] -> 
         D = Data,
         %io:format("find_entry binary ~p~n", [D]),
         {D, Index, Count}
    end.

%% @doc This function reads the next entry from the log at the given location
%% and returns {entry, Entry, NewLocation}. If the end of file has been reached,
%% return eof to the client. Errors are fail-fast.
-spec read_entry(ets:tid(), non_neg_integer()) ->
    {entry, binary(), non_neg_integer()} | {skip, non_neg_integer()} | eof.
read_entry(Data, Location) ->
    read_data(Data, Location).


-spec read_data(ets:tid(), non_neg_integer()) ->
    {entry, binary(), non_neg_integer()} | eof.
read_data(Table, Location) ->
    case ets:lookup(Table, Location) of
        [{Location, Entry}] ->
            %% Fail-fast Integrity check. TODO: Offer user repair options?
            NewLocation = Location + 1,
            {entry, Entry, NewLocation};
        [] ->
            eof
    end.
