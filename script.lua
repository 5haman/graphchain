local id = nil
local edges = {}
local obj = {}
local pairs = {}
local response = {}
local Tx = cjson.decode( KEYS[1] )
local s, d, t
local sameowner

local function CheckOwner( Input, Output )
  if Input[1] ~= 'coinbase' and table.getn( Input ) >= 2 and table.getn( Output ) == 1 then
    return true
  end
  return false
end

local function GetId( Input )
  for _, src in ipairs( Input ) do
    if redis.call('exists', src) == 1 then
      return redis.call('get', src)
    end
  end
  return nil
end

local function CheckId( Id )
  if Id == nil then
    return string.format("%d", redis.call('incr', 'id'))
  end
  return Id
end

local function TableConcat( t1, t2 )
  for i = 1, #t2 do
    t1[ #t1 + 1 ] = t2[ i ]
  end
  return t1
end

sameowner = CheckOwner( Tx['in'], Tx['out'] )
if sameowner then
  id = GetId( Tx['in'] )
end

for _, src in ipairs(Tx['in']) do
  if sameowner == false and redis.call('exists', src) == 0 then
    id = CheckId( id )
    redis.call('set', src, id)
    --redis.call('SADD', id, src)
    obj['id'] = id
    obj['addr'] = src
    table.insert(edges, obj)
  else
    id = CheckId( id )
    redis.call('set', src, id)
  end
end

obj = {}
for _, dst in ipairs(Tx['out']) do
  if redis.call('exists', dst) == 0 then
    id = CheckId( id )
    redis.call('set', dst, id)
    obj['id'] = id
    obj['addr'] = dst
    table.insert(edges, obj)
  end
end

local n = table.getn( Tx['in'] )
local max = 5000
s = {}
if n > max then
  for i = 1, n-max, max do
    TableConcat(s, redis.call('MGET', unpack( Tx['in'], i, i + max )))
  end
  local m = table.getn( s )
  TableConcat(s, redis.call('MGET', unpack( Tx['in'], m, n )))
else
  s = redis.call('MGET', unpack(Tx['in']))
end
d = redis.call('MGET', unpack(Tx['out']))

for _, src in ipairs( s ) do
  for _, dst in ipairs( d ) do
    if src ~= dst then
      t = string.format("%s,%s", src, dst)
      if redis.call('EXISTS', t) == 0 then
        redis.call('SET', t, 1)
        table.insert(pairs, t)
      end
    end
  end
end

response['edges'] = edges
response['pairs'] = pairs
response['sameowner'] = sameowner

return cjson.encode(response)
