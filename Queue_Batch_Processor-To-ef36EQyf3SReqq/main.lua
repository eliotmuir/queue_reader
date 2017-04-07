-- This imports the module that allows us to do bulk reads of the queue
-- using the Log API
local bulk = require 'queue.api'

-- This call gives us a list of the channels which are feeding into this
-- channel.  I call it outside of main to avoid the overhead of doing this
-- more than once.
local SourceList = bulk.queues()

function main(Data) 
   -- Uncomment this line to reset our queue pointer
   -- to be at the start of the queue.
   --bulk.reset(SourceList[1])
   
   -- This call fetches the list of messages sitting in
   -- the queue.
   local MessageList = bulk.fetch(SourceList[1])
   if #MessageList.messages == 0 then
      -- No messages to process
      return 
   end

   trace(#MessageList.messages)
   
   -- We 'Process' the messages with this log message
   -- I use the XYZ word to make the message easy to find
   iguana.logInfo("XYZ - Processed "
         ..#MessageList.messages.." messages.")
   
   -- This modifies the stored location of where we last read from
   -- the queue so that we can avoid processing them again.
   bulk.commit(MessageList)
end