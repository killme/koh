local odbxuv = require "odbxuv"
local createQueryBuilder = require "odbxuv.queryBuilder".createQueryBuilder
local mq = require "koh.mq"

mq.exchange("forceRedraw")

local sql = odbxuv.Connection:new()

local function queuNextBatch()
    local q = createQueryBuilder(sql)

    q:select({"player", "id"}, "uuid", "layer", "url", "texture")
    q:from("player", "player_texture")
    q:where("player.id = player_id")

    q:finalize(function(err, q)
        if err then
            print ("Error during fetch query generation", err)
        else
            sql:query(q, function(err, q)
                q:on("close", function()

                end)

                if err then
                    print ("Error during fetch query ", err)
                    q:close()
                else
                    q:on("fetch", function() end)
                    q:on("row", function(id, uuid, layer, url, texture)
                        mq.publish(mq.QUEUE.IMAGE_PROCESSING, function()
                            mq.write(mq.TAG.RENDER_IMG, {
                                id = id,
                                uuid = uuid,
                                layer = layer,
                                url = url,
                                texture = texture
                            })
                        end)
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

sql:connect (dofile("database.conf"), function(err, ...)
    if err then
        error(err)
    end

    queuNextBatch()
end)

sql:on("error", function(...)
    print("SQL error:", ...)
    p(...)
end)

sql:on("close", function()

end)
