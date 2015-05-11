local table = require "table"
local process = require "luvit.process"
local ibmt = require "ibmt"
local odbxuv = require "odbxuv"
local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder

local sql = odbxuv.Connection:new()
local startupTask = ibmt.create()

local BufferedStream = require "luvit.core".iStream:extend()

function BufferedStream:initialize()
    self.paused = true
    self.buffer = {}
    self.finished = false
    self.pattern = "(.-)\n"
    self.bufferPattern = "\n([^\n]*)$"
    self.lastLine = ""
    self.ended = false
end

function BufferedStream:write(data)
    data = self.lastLine .. data
    self.lastLine = ""

    for line in data:gmatch(self.pattern) do
        self:enQueue(line)
    end

    self.lastLine = data:gmatch(self.bufferPattern)()
end

function BufferedStream:enQueue(line)
    if self.paused then
        self.buffer[#self.buffer+1] = line
    else
        self:emit("line", line)
    end
end

function BufferedStream:done()
    if self.lastLine ~= "" then
        self:enQueue(self.lastLine)
        self.lastLine = ""
    end

    self.done = true

    self:_checkEnd()
end

function BufferedStream:resume()
    self.paused = false

    for k, line in ipairs(self.buffer) do
        table.remove(self.buffer, k)
        self:emit("line", line)

        if self.paused then
            break
        end
    end

    self:_checkEnd()
end

function BufferedStream:_checkEnd()
    if not self.paused and self.done and #self.buffer == 0 then
        self:emit("end")
    end
end

function BufferedStream:pause()
    self.paused = true
end

local bufferedStream = BufferedStream:new()

process.stdin:pipe(bufferedStream)

bufferedStream:on("line", function(uuid)
    local q = createQueryBuilder(sql)

    q:select("id")
    q:from("player")
    q:where("UUID = :UUID")
    q:bind("UUID", uuid)

    q:finalize(function(err, q)
        if err then
            error(err)
        end

        sql:query(q, function(err, q)
            if err then
                error(err)
            end

            q:on("row", function(id)
                print ("User " .. uuid .. " already exists with id " .. tostring(id))
            end)

            q:on("fetched", function()
                q:close(function()
                    bufferedStream:resume()
                end)
            end)

            q:on("fetch", function(...)
                if q:getAffectedCount() == 0 then
                    local q = createQueryBuilder(sql)

                    q:insert("UUID", "lastNameFetch", "lastProfileFetch")
                    q:into("player")
                    q:values(uuid, 0, 0)
                    q:finalize(function(err, q)
                        if err then
                            print ("Failed to insert new user ("..uuid.."), querybuilder failed " .. tostring(err))
                        else
                            sql:query(q, function(err, q)
                                if err then
                                    print ("Failed to insert new user ("..uuid.."), query failed " .. tostring(err))
                                    q:close(function() end)
                                else
                                    q:on("fetched", function()
                                        p "FETCH"
                                        q:close(function()
                                            p "CLOSED B"
                                        end)
                                    end)

                                    q:on("fetch", function()
                                        local affected = q:getAffectedCount()

                                        if affected ~= 1 then
                                            print ("Failed to insert new user ("..uuid.."), invalid affected row count: " .. affected)
                                        else
                                            print ("Added new user: ", uuid)
                                        end
                                    end)

                                    q:fetch()
                                end
                            end)
                        end
                    end)
                end
            end)

            q:fetch()
        end)
    end)


    bufferedStream:pause()
end)

bufferedStream:on("end", function()
    print "Shutting down ..."
    sql:close()
end)


startupTask:on("finish", function()
    bufferedStream:resume()
end)

startupTask:push()
sql:connect (dofile("database.conf"), function(err, ...)
    if err then
        return startupTask:cancel(err)
    end

    startupTask:pop()
end)

sql:on("error", function(...)
    print("SQL error:", ...)
    p(...)
end)

sql:on("close", function()

end)
