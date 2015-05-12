local table = require "table"
local coroutine = require "coroutine"
local mq = require "koh.mq"
local JSON = require "luvit.json"
local url = require "luvit.url"
local http = require "luvit.http"
local https = require "luvit.https"
local base64 = require "luautil.base64"

mq.exchange("amazonFetcher")

local listener = coroutine.create(function()
    while true do
        mq.listen({mq.QUEUE.AMAZON_S3_API}, function(consumer_tag, tag, data)
            local response = coroutine.yield(tag, data)
            data.response = response

            mq.publish(mq.QUEUE.GENERAL_PROCESSING, function()
                print ("Emit callback", data.callbackTag, data.url)
                mq.write(data.callbackTag, data)
            end)
        end)
        print "Restart listening"
    end
end)

local function doNext(v, tag, data)
    assert(tag == mq.TAG.FETCH_URL or tag == "dl_skin", "Invalid tag: " .. tag .. " != " .. mq.TAG.FETCH_URL)

    local url = url.parse(data.url)

    local lib = https

    if url.protocol == "http" then
        lib = http
    end

    local req = lib.request(url, function(res)
        assert(res.statusCode == 200, "Invalid status code: " .. res.statusCode)

        local buf = {}

        res:on("data", function(chunk)
            buf[#buf+1] = chunk
        end)

        res:on("end", function()
            return doNext(coroutine.resume(listener, base64.encode(table.concat(buf, ""))))
        end)
    end)

    req:done()
end

doNext(coroutine.resume(listener))
