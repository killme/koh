local os = require "os"
local table = require "table"
local process = require "luvit.process"
local ibmt = require "ibmt"
local odbxuv = require "odbxuv"
local mq = require "koh.mq"
local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder
local setTimeout = require "luvit.timer".setTimeout
local epoch = require "epoch"

mq.exchange("profileScheduler")

local sql = odbxuv.Connection:new()
local startupTask = ibmt.create()

local ONE_DAY = 86400
local RETRY_INTERVAL = ONE_DAY / 2

local NAME_FETCH_INTERVAL = ONE_DAY
local PROFILE_FETCH_INTERVAL = ONE_DAY * 2

local function enqueueRow(id, uuid, tag)
    local q = createQueryBuilder(sql)
    q:update("player")
    q:set({lastEnqueued = epoch()})
    q:where("UUID = :uuid")
    q:bind("uuid", uuid)
    q:finalize(function(err, q)
        if err then
            print ("Error during update generation for " .. uuid, err)
        else
            sql:query(q, function(err, q)
                q:on("close", function() end)
                if err then
                    print("Error during updating of " .. uuid, err)
                    q:close()
                else
                    q:on("fetch", function()
                        if q:getAffectedCount() ~= 1 then
                            print ("Failed to update time affected count: " .. q:getAffectedCount())
                        end
                    end)
                    q:on("fetched", function()
                        q:close()
                    end)
                    q:fetch()
                end
            end)
        end
    end)

    mq.publish(mq.QUEUE.GENERAL_PROCESSING, function()
        mq.write(tag, { uuid = uuid, id = id })
    end)

    print ("Enqueued", tag, uuid, id)
end

local function queuNextBatch()
    local q = createQueryBuilder(sql)

    q:select("id", "UUID", "lastNameFetch", "lastProfileFetch")
    q:from("player")
    q:where("lastNameFetch < :NAME_FETCH_INTERVAL AND lastProfileFetch < :PROFILE_FETCH_INTERVAL AND lastEnqueued < :RETRY_INTERVAL")
    q:limit(30)
    q:bind("NAME_FETCH_INTERVAL", epoch() - NAME_FETCH_INTERVAL)
    q:bind("PROFILE_FETCH_INTERVAL", epoch() - PROFILE_FETCH_INTERVAL)
    q:bind("RETRY_INTERVAL", epoch() - RETRY_INTERVAL)

    q:finalize(function(err, q)
        if err then
            print ("Error during fetch query generation", err)
        else
            sql:query(q, function(err, q)
                q:on("close", function()
                    setTimeout(30000, queuNextBatch)
                end)

                if err then
                    print ("Error during fetch query ", err)
                    q:close()
                else
                    q:on("fetch", function() end)
                    q:on("row", function(id, uuid, lastNameFetch, lastProfileFetch)
                        local fetchName = tonumber(lastNameFetch) < NAME_FETCH_INTERVAL
                        local fetchProfile = tonumber(lastProfileFetch) < PROFILE_FETCH_INTERVAL
                        enqueueRow(tonumber(id), uuid,
                                (fetchName == fetchProfile) and mq.TAG.PRE_FETCH_ALL
                            or  (fetchName and mq.TAG.PRE_FETCH_NAMES)
                            or  (fetchProfile and mq.TAG.PRE_FETCH_PROFILE))
                    end)
                    q:on("fetched", function()
                        q:close()
                    end)
                    q:fetch()
                end
            end)
        end
    end)
end

startupTask:on("finish", function()
    queuNextBatch()
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
