local coroutine = require "coroutine"
local mq = require "koh.mq"
local JSON = require "luvit.json"
local url = require "luvit.url"
local http = require "luvit.http"
local https = require "luvit.https"
local odbxuv = require "odbxuv"
local epoch = require "luautil.epoch"
local ibmt = require "ibmt"

local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder
local sql = odbxuv.Connection:new()

mq.exchange("mojangFetcher")

local listener = coroutine.create(function()
    while true do
        mq.listen({mq.QUEUE.DATABASE_UPDATER}, function(consumer_tag, tag, data)
            coroutine.yield(tag, data)
        end)
        print "Restart listening"
    end
end)

local function doDatabaseUpdate(d, tag, data)
    if tag == mq.TAG.UPDATE_PROFILE_TIME then
        local q = createQueryBuilder(sql)
        q:update("player")
        q:set({lastProfileFetch = epoch()})
        q:where("id = :id")
        q:limit(1)
        q:bind("id", data.id)

        q:finalize(function(err, q)
            if err then
                error(err)
            end

            sql:query(q, function(err, q)
                if err then
                    error(err)
                else
                    q:close(function()
                        print ("Profile update", data.uuid, data.id)
                        doDatabaseUpdate(coroutine.resume(listener))
                    end)
                end
            end)
        end)
    elseif tag == mq.TAG.PROCESS_NAMES then
        local updateNamesTask = ibmt.create()

        updateNamesTask:push()

        do
            local q = createQueryBuilder(sql)
            q:update("player")
            q:set({lastNameFetch = epoch()})
            q:where("id = :id")
            q:limit(1)
            q:bind("id", data.id)

            updateNamesTask:push()
            q:finalize(function(err, q)
                if err then
                    error(err)
                end

                sql:query(q, function(err, q)
                    if err then
                        error(err)
                    else
                        q:close(function()
                            print ("Name update", data.uuid, data.id)
                            updateNamesTask:pop()
                        end)
                    end
                end)
            end)
        end

        for i, value in pairs(data.names) do
            updateNamesTask:push()
            local q = createQueryBuilder(sql)
            q:insert("player_id", "name", "start_time")
            q:ignore()
            q:into("player_name")
            q:values(data.id, value.name, value.changedToAt or 0)
            q:finalize(function(err, q)
                if err then
                    error(err)
                else
                    sql:query(q, function(err, q)
                        if err then
                            error(err)
                        else
                            print ("Inserted ", value.name, "with time", value.changedToAt or 0, "for", data.uuid, data.id)
                            q:close(function()
                                updateNamesTask:pop()
                            end)
                        end
                    end)
                end
            end)
        end

        updateNamesTask:on("finish", function()
            doDatabaseUpdate(coroutine.resume(listener))
        end)

        updateNamesTask:pop()
    elseif tag == mq.TAG.PROCESS_SKIN then
        local q = createQueryBuilder(sql)

        q:select("id")
        q:from("player_texture")
        q:where("player_id = :player_id AND url = :url AND layer = :layer")
        q:bind("player_id", data.id)
        q:bind("url", data.url)
        q:bind("layer", data.layer)

        local uploadTextureTask = ibmt.create()

        uploadTextureTask:on("finish", function()
            doDatabaseUpdate(coroutine.resume(listener))
        end)

        uploadTextureTask:push()
        q:finalize(function(err, q)
            if err then
                error(err)
            else
                sql:query(q, function(err, q)
                    if err then
                        error(err)
                    else
                        q:on("fetch", function()
                            if 0 == q:getAffectedCount() then
                                print ("Uploading new texture", data.uuid, data.id, data.url)
                                local q = createQueryBuilder(sql)
                                q:insert("player_id", "url", "layer", "texture", "timestamp")
                                q:into("player_texture")
                                q:values(data.id, data.url, data.layer, data.texture, epoch())

                                uploadTextureTask:push()
                                q:finalize(function(err, q)
                                    if err then
                                        error(err)
                                    else
                                        sql:query(q, function(err, q)
                                            if err then
                                                error(err)
                                            else
                                                q:close(function()
                                                    uploadTextureTask:pop()
                                                end)
                                            end
                                        end)
                                    end
                                end)
                            else
                                print ("Texture already up to date", data.uuid, data.id)
                            end
                        end)

                        q:on("row", function() end)

                        q:on("fetched", function()
                            q:close(function()
                                uploadTextureTask:pop()
                            end)
                        end)

                        q:fetch()
                    end
                end)
            end
        end)
    else
        p(d, tag, data, mq.PROCESS_SKIN)
    end
end

sql:connect (dofile("database.conf"), function(err, ...)
    if err then
        error(err)
    end

    doDatabaseUpdate(coroutine.resume(listener))
end)
