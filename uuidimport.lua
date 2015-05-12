local fs = require "luvit.fs"
local table = require "table"
local process = require "luvit.process"
local ibmt = require "ibmt"
local odbxuv = require "odbxuv"
local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder

local sql = odbxuv.Connection:new()
local startupTask = ibmt.create()

local function import(uuid, cb)
    local q = createQueryBuilder(sql)
    uuid = uuid:sub(0, 36)

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
                    cb()
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
                                        q:close(function() end)
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
end

local processOne
do
    local t = fs.readdir(process.argv[1], 0)
    local k, ent

    processOne = function()
        k, ent = next(t, k)

        if k ~= nil then
            if ent.type == "FILE" then
                import(ent.name, processOne)
            else
                processOne()
            end
        else
            sql:close()
        end
    end
end

startupTask:on("finish", processOne)

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

