local store = require 'store2'

local Username = 'admin'
local Password = 'password'

local S = store.connect(iguana.project.guid())

local bulk = {}

function bulk.queues()
   local Config = iguana.channelConfig{guid=iguana.channelGuid()}
   Config = xml.parse{data=Config}
   trace(Config)
   local List = {}
   for i=1, Config.channel.to_mapper.dequeue_list:childCount("dequeue") do
      List[i] = Config.channel.to_mapper.dequeue_list:child("dequeue", i).source_name:S()
   end
   return List
end

local PositionCache = {}

local function FetchQueuePosition(ChannelName)
   if PositionCache[ChannelName] then
      return PositionCache[ChannelName]
   end
   local Position = S:get(ChannelName)
   if not Position then
      Position = {after="1971/01/01 00:00:00", file=19710101, pos=0}
   else
      Position = json.parse{data=Position}
   end
   PositionCache[ChannelName] = Position
   return Position
end


local function MessageGreaterThan(ID, Position)
   trace(Position.pos)
   local MsgId = ID:split("-")
   if tonumber(MsgId[1]) > Position.file then
      return true
   end
   if tonumber(MsgId[2]) > Position.pos then
      return true
   end
   return false
end

local function FindFirstUnprocessedMessage(Data, Position)
   for i=1, Data.export:childCount("message") do
      if MessageGreaterThan(Data.export:child("message",i).message_id:nodeValue(), Position) then
         return i   
      end
   end
   return Data.export:childCount("message") +1
end

local function QueueData(ChannelName, Position)
   local Result = {}
   Result.messages = {}
   Result.source = ChannelName
   if not iguana.isTest() and not MessageGreaterThan(iguana.messageId(), Position) then
      -- trick to return before invoking expense of http.get call.
      return Result
   end
   local BeforeTime = os.ts.time()
   local before = os.ts.date("%Y/%m/%d %H:%M:%S", BeforeTime)
   local X = net.http.get{url='http://localhost:6543/api_query',
      parameters={
         username=Username,
         password=Password,
         type    = 'messages',    
         source  = ChannelName,  
  
         --reverse = 'false',       
         deleted = 'false',
         after = Position.after,
         before = BeforeTime
      },live=true}  
   X = xml.parse{data=X}
   os.ts.time()
   if X.export:childCount("message") == 0 then
      return Result
   end
   local FromIndex = FindFirstUnprocessedMessage(X, Position)
   local LastIndex = X.export:childCount("message")
   local RefParts = X.export:child("message", LastIndex).message_id:nodeValue():split("-")
   local After = X.export:child("message", LastIndex).time_stamp:nodeValue():sub(1,-5)
   Result.last_position = { after=before, file=tonumber(RefParts[1]), pos=tonumber(RefParts[2])}
   os.ts.time()
   for i=FromIndex,LastIndex do
      Result.messages[i+1-FromIndex] = X.export:child("message", i).data:nodeValue()
   end
   os.ts.time()
   return Result
end

function bulk.fetch(ChannelName)
   local Position = FetchQueuePosition(ChannelName)
   local Result =  QueueData(ChannelName, Position)
   return Result, #Result.messages
end

function bulk.commit(Set)
   if #Set.messages == 0 then
     return 
   end
   if not iguana.isTest() then
      S:put(Set.source, json.serialize{data=Set.last_position})
      PositionCache[Set.source] = Set.last_position
   end
end

function bulk.reset(Name)
   S:put(Name, nil)
end

return bulk