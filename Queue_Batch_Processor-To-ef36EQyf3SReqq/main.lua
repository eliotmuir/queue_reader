local bulk = require 'queue.api'
local SourceList = bulk.queues()

function main(Data) 
   trace(List)
   -- Uncomment this line to process the list
   --bulk.reset(SourceList[1])
   local MessageList = bulk.fetch(SourceList[1])
   if #MessageList.messages == 0 then
      -- No messages to process
      return 
   end

   trace(#MessageList.messages)
   
   iguana.logInfo("XYZ - Processed "
         ..#MessageList.messages.." messages.")
   bulk.commit(MessageList)
end