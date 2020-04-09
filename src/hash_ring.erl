-module(hash_ring).

-behavior(gen_server).

%-define(CONFIG_FILE, "./consistent_hashing.config").

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-export([start_link/1,
		stop/0]).

-export([get_ring/0,
		 get_ring_keys/0,
		 get_hash_mask/0,
		 add_node/4, 
		 add_node/5, 
		 delete_node/1,
		 initialize/1,
		 get_node/1]).

-record(node_conf, {
		  hostname,
		  ip,
		  port,
		  vnodes=0
		 }).

-record(consistent_hashing_state, {
		  hash_function=phash2,
		  max_hash_byte_size=4,
		  hash_mask,
		  hash_ring=maps:new(),
		  keys=[],
		  nodes=maps:new()
		 }).

add_node(Nodename, Hostname, IP, Port) ->
	add_node(Nodename, Hostname, IP, Port, 0).
add_node(Nodename, Hostname, IP, Port, VNodes) ->
	Config = #node_conf{hostname=Hostname, ip=IP, port=Port, vnodes=VNodes},
	gen_server:call(?MODULE, {add_node, Nodename, Config}).

delete_node(Nodename) ->
	gen_server:call(?MODULE, {delete_node, Nodename}).

get_node(Key) ->
	gen_server:call(?MODULE, {get_node, Key}).

initialize(ConfigFile) ->
	Config = case file:consult(ConfigFile) of
		{ok, Conf} -> Conf;
		{error, Reason} -> io:format("Cannot read the file. Reason: ~p", [Reason]), []
	end,
	NodesConfig = proplists:get_value(nodes, Config),
	add_nodes(NodesConfig).

add_nodes([NodeConfig | NodesConfig]) ->
	{Nodename, Config} = NodeConfig,
	Hostname = proplists:get_value(hostname, Config),
	IP = proplists:get_value(ip, Config),
	Port = proplists:get_value(port, Config),
	VNodes = proplists:get_value(vnodes, Config),
	add_node(Nodename, Hostname, IP, Port, VNodes),
	add_nodes(NodesConfig);
add_nodes([]) ->
	ok.

get_ring() ->
	gen_server:call(?MODULE, get_ring).

get_ring_keys() ->
	gen_server:call(?MODULE, get_ring_keys).

get_hash_mask() ->
	gen_server:call(?MODULE, get_hash_mask).

handle_call(get_ring, _From, #consistent_hashing_state{hash_ring=HashRing}=State) ->
	{reply, HashRing, State};

handle_call(get_ring_keys, _From, #consistent_hashing_state{keys=Keys}=State) ->
	{reply, Keys, State};

handle_call(get_hash_mask, _From, #consistent_hashing_state{hash_mask=HashMask}=State) ->
	{reply, HashMask, State};

handle_call({add_node, Nodename, #node_conf{vnodes=VirtualNodeCount}=Config}, _From, 
			#consistent_hashing_state{hash_function=HashFunction, 
									  hash_ring=HashRing,
									  hash_mask=HashMask,
									  nodes=Nodes}=State) ->
	HashRing1 = insert_nodes(Nodename, HashRing, HashFunction, HashMask, VirtualNodeCount),
	Keys = maps:keys(HashRing1),
	SortedKeys = lists:sort(Keys),
	Nodes1 = Nodes#{Nodename => Config},
	State1 = State#consistent_hashing_state{hash_ring=HashRing1, keys=SortedKeys, nodes=Nodes1},
	{reply, ok, State1};

handle_call({delete_node, Nodename}, _From,
			#consistent_hashing_state{hash_function=HashFunction, 
									  hash_ring=HashRing,
									  hash_mask=HashMask}=State) ->
	HashRing1 = delete_nodes(Nodename, HashRing, HashFunction, HashMask),
	Keys = maps:keys(HashRing1),
	SortedKeys = lists:sort(Keys),
	State1 = State#consistent_hashing_state{hash_ring=HashRing1, keys=SortedKeys},
	{reply, ok, State1};

handle_call({get_node, Key}, _From, 
			#consistent_hashing_state{hash_function=HashFunction,
									  hash_mask=HashMask,
									  keys=Keys,
									  nodes=Nodes,
									  hash_ring=HashRing} = State) ->
	Hash = hash(HashFunction, Key, HashMask),
	Nodename = find_node(Hash, Keys, HashRing),
	Node = {Nodename, maps:get(Nodename, Nodes)},
	{reply, Node, State};

handle_call(stop, _From, State) ->
    {stop, normalStop, State};

handle_call(_Msg, _From, State) ->
	{noreply, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

start_link(ConfigFile) ->
	case gen_server:start_link({local, ?MODULE}, ?MODULE, [ConfigFile], []) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid}
    end.

init([ConfigFile]) ->
	{ok, Config} = file:consult(ConfigFile),
	ConfigMap = maps:from_list(Config),
    MaxHashByteSize	= maps:get(max_hash_byte_size, ConfigMap, 4),
	HashMask = (1 bsl MaxHashByteSize * 8) - 1,
	{ok, #consistent_hashing_state{
			hash_function=maps:get(hash_function, ConfigMap, phash2),
			hash_mask=HashMask}}.

stop() ->
    gen_server:call(?MODULE, stop).

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

hash(HashAlgorithm, Nodename, HashMask) ->
	calc_hash(HashAlgorithm, Nodename) band HashMask.

calc_hash(crc32, Nodename) ->
	erlang:crc32(term_to_binary(Nodename));
calc_hash(phash2, Nodename) ->
	erlang:phash2(Nodename, 16#100000000);
calc_hash(HashAlgorithm, Node) ->
    crypto:bytes_to_integer(crypto:hash(HashAlgorithm, term_to_binary(Node))).

insert_nodes(Nodename, HashRing, HashFunction, HashMask, VirtualNodeCount) ->
	insert_nodes(Nodename, HashRing, HashFunction, HashMask, VirtualNodeCount, 0).

insert_nodes(_Nodename, HashRing, _HashFunction, _HashMask, 
			 VirtualNodeCount, Index) when Index > VirtualNodeCount ->
	HashRing;
insert_nodes(Nodename, HashRing, HashFunction, HashMask,
             VirtualNodeCount, Index) ->
	Nodename1 = Nodename ++ "-" ++ erlang:integer_to_list(Index),
	Hash = hash(HashFunction, Nodename1, HashMask),
	HashRing1 = insert_node(Nodename, Hash, HashRing),
	insert_nodes(Nodename, HashRing1, HashFunction, HashMask,
             VirtualNodeCount, Index+1).

insert_node(Nodename, Hash, HashRing) ->
	% Same as Hash rem RingSize but more efficient.
	% Assuming the RingSize is a power of 2.
	%Hash1 = Hash band (RingSize-1),
	try
		maps:get(Hash, HashRing),
		error({key_already_exist, Nodename})
	catch
		error:{badkey, Hash} ->
			HashRing#{Hash => Nodename}
	end.

delete_nodes(Nodename, HashRing, HashFunction, HashMask) ->
    delete_nodes(Nodename, HashRing, HashFunction, HashMask, 0).

delete_nodes(Nodename, HashRing, HashFunction, HashMask, Index) ->
	Nodename1 = Nodename ++ "-" ++ erlang:integer_to_list(Index),
	Hash = hash(HashFunction, Nodename1, HashMask),
	try
		maps:get(Hash, HashRing),
		HashRing1 = maps:remove(Hash, HashRing), 
		delete_nodes(Nodename, HashRing1, HashFunction, HashMask, Index+1)
	catch
		error:{badkey, Hash} ->
			HashRing
	end.


find_node(_Hash, [], _HashRing) -> none;
find_node(Hash, [Key | _Rest] = Keys, HashRing) ->
	case find_node1(Hash, Keys, HashRing) of
		none -> maps:get(Key, HashRing);
		Node -> Node
	end.

find_node1(_Hash, [], _HashRing) -> none;
find_node1(Hash, [Key | _Keys], HashRing) when Hash =< Key ->
	maps:get(Key, HashRing);
find_node1(Hash, [_Key | Keys], HashRing) ->
	find_node1(Hash, Keys, HashRing).
