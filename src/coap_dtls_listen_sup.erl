-module(coap_dtls_listen_sup).

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(InPort, Opts0) ->
    Opts =  Opts0 ++ [binary, {protocol, dtls}, {reuseaddr, true}],
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, SocketSup} = supervisor:start_child(Sup, 
    	{socket_sup, {coap_dtls_socket_sup, start_link, []}, 
    	 transient, infinity, supervisor, [coap_dtls_socket_sup]}),
    {ok, _Listener}  = supervisor:start_child(Sup, 
    	{listener, {coap_dtls_listen, start_link, [SocketSup, InPort, Opts]},
    	 transient, 16#ffffffff, worker, [coap_dtls_listen]}),
    {ok, Sup}. 

init([]) ->
    {ok, {{rest_for_one, 10, 3600}, []}}.



