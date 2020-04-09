-module(distribution).

-export([calculate/0]).

calculate() ->
	Keys = hash_ring:get_ring_keys(),
	VirtualNodes = hash_ring:get_ring(),
	calculate(Keys, VirtualNodes).

calculate([], _VirtualNodes) -> ok;
calculate(Keys, VirtualNodes) ->
	LastKey = lists:last(Keys),
	RangeMap = calculate(LastKey, Keys, VirtualNodes, #{}),
	RangeSum = maps:fold(fun(_K, V, Sum) -> Sum + V end, 0, RangeMap),
	Percentages = maps:map(fun(_K, V) -> V / RangeSum end, RangeMap),
	Percentages.

calculate(_PrevKey, [], _VirtualNodes, Map) ->
	Map;
calculate(PrevKey, [Key | Keys], VirtualNodes, Map) when PrevKey > Key ->
	HashMask = hash_ring:get_hash_mask(),
	Nodename = maps:get(Key, VirtualNodes),
	Range = maps:get(Nodename, Map, 0),
	Range1 = Range + HashMask - PrevKey + Key,
	Map1 = maps:put(Nodename, Range1, Map),
	calculate(Key, Keys, VirtualNodes, Map1);
calculate(PrevKey, [Key | Keys], VirtualNodes, Map) ->
	Nodename = maps:get(Key, VirtualNodes),
	Range = maps:get(Nodename, Map, 0),
	Range1 = Range + Key - PrevKey,
	Map1 = maps:put(Nodename, Range1, Map),
	calculate(Key, Keys, VirtualNodes, Map1).
