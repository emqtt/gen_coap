%
% The contents of this file are subject to the Mozilla Public License
% Version 1.1 (the "License"); you may not use this file except in
% compliance with the License. You may obtain a copy of the License at
% http://www.mozilla.org/MPL/
%
% Copyright (c) 2015 Petr Gotthard <petr.gotthard@centrum.cz>
%

% dispatcher for UDP communication
% maintains a lookup-table for existing channels
% when a channel pool is provided (server mode), creates new channels
-module(coap_udp_socket).
-behaviour(gen_server).

-export([start_link/0, start_link/2, get_channel/2, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).
-export([delete_channel/1]).

-record(state, {sock, pool}).

-define(TAB, ets_coap_channel_pid).

-define(LINUX_SOL_SOCKET, 1).
-define(LINUX_SO_REUSEPORT, 15).
-define(UNIX_SOL_SOCKET, 16#FFFF).
-define(UNIX_SO_REUSEPORT, 16#0200).

-define(OPT_BUFFER, 65536*8).

% client
start_link() ->
    gen_server:start_link(?MODULE, [0], []).
% server
start_link(InPort, SupPid) ->
    gen_server:start_link(?MODULE, [InPort, SupPid], []).

get_channel(Pid, {PeerIP, PeerPortNo}) ->
    gen_server:call(Pid, {get_channel, {PeerIP, PeerPortNo}}).

close(Pid) ->
    % the channels will be terminated by their supervisor (server), or
    % should be terminated by the user (client)
    gen_server:cast(Pid, shutdown).


init([InPort]) ->
    {ok, Socket} = gen_udp:open(InPort, [binary, {active, once}, {buffer, ?OPT_BUFFER*2}, {recbuf, ?OPT_BUFFER}, {sndbuf, ?OPT_BUFFER}, {reuseaddr, true}]++reuseport()),
    %{ok, InPort2} = inet:port(Socket),
    %error_logger:info_msg("coap listen on *:~p~n", [InPort2]),
    {ok, #state{sock=Socket}};

init([InPort, SupPid]) ->
    gen_server:cast(self(), {set_pool, SupPid}),
    init([InPort]).

handle_call({get_channel, ChId}, _From, State=#state{pool=undefined}) ->
    case find_channel(ChId) of
        {ok, Pid} ->
            {reply, {ok, Pid}, State};
        undefined ->
            {ok, _, Pid} = coap_channel_sup:start_link(self(), ChId),
            store_channel(ChId, Pid),
            {reply, {ok, Pid}, State}
    end;
handle_call({get_channel, ChId}, _From, State=#state{pool=PoolPid}) ->
    case coap_channel_sup_sup:start_channel(PoolPid, ChId) of
        {ok, _, Pid} ->
            store_channel(ChId, Pid),
            {reply, {ok, Pid}, State};
        Error ->
            {reply, Error, State}
    end;
handle_call(_Unknown, _From, State) ->
    {reply, unknown_call, State}.

handle_cast({set_pool, SupPid}, State) ->
    % calling coap_server directly from init/1 causes deadlock
    PoolPid = coap_server:channel_sup(SupPid),
    {noreply, State#state{pool=PoolPid}};
handle_cast(shutdown, State) ->
    {stop, normal, State};
handle_cast(Request, State) ->
    io:fwrite("coap_udp_socket unknown cast ~p~n", [Request]),
    {noreply, State}.

handle_info({udp, Socket, PeerIP, PeerPortNo, Data}, State=#state{pool=PoolPid}) ->
    ChId = {PeerIP, PeerPortNo},
    inet:setopts(Socket, [{active, once}]),
    case find_channel(ChId) of
        % channel found in cache
        {ok, Pid} ->
            Pid ! {datagram, Data},
            {noreply, State};
        undefined when is_pid(PoolPid) ->
            case coap_channel_sup_sup:start_channel(PoolPid, ChId) of
                % new channel created
                {ok, _, Pid} ->
                    Pid ! {datagram, Data},
                    store_channel(ChId, Pid),
                    {noreply, State};
                {error, {already_started, SuPid}} ->
                    % this process is created, and its pid is writing into ets table now, and find_channel() will return undefined
                    ChPid = coap_channel_sup:get_channel(SuPid),
                    ChPid ! {datagram, Data},
                    {noreply, State};
                % drop this packet
                {error, _} ->
                    {noreply, State}
            end;
        undefined ->
            % ignore unexpected message received by a client
            % TODO: do we want to send reset?
            {noreply, State}
    end;
handle_info({datagram, {PeerIP, PeerPortNo}, Data}, State=#state{sock=Socket}) ->
    ok = gen_udp:send(Socket, PeerIP, PeerPortNo, Data),
    {noreply, State};
handle_info(Info, State) ->
    io:fwrite("coap_udp_socket unexpected ~p~n", [Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{sock=Sock}) ->
    gen_udp:close(Sock),
    ok.


find_channel(ChId) ->
    case ets:lookup(?TAB, ChId) of
        % there is a channel in our cache, but it might have crashed
        [{ChId, Pid}] ->
            case erlang:is_process_alive(Pid) of
                true  -> {ok, Pid};
                false -> undefined
            end;
        % we got data via a new channel
        [] -> undefined
    end.

store_channel(ChId, Pid) ->
    ets:insert(?TAB, {ChId, Pid}).

delete_channel(ChId) ->
    ets:delete(?TAB, ChId).


reuseport() ->
    reuseport(os:type()).

reuseport({unix, linux})   -> [{raw, ?LINUX_SOL_SOCKET, ?LINUX_SO_REUSEPORT, <<1:32/native>>}];  % require linux kernel v3.9 and later version
reuseport({unix, darwin})  -> [{raw, ?UNIX_SOL_SOCKET, ?UNIX_SO_REUSEPORT, <<1:32/native>>}];
reuseport({unix, freebsd}) -> reuseport({unix, darwin});
reuseport({unix, openbsd}) -> reuseport({unix, darwin});
reuseport({unix, netbsd})  -> reuseport({unix, darwin});
reuseport(_)               -> [].



% end of file
