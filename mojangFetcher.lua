local coroutine = require "coroutine"
local mq = require "koh.mq"
local JSON = require "luvit.json"
local url = require "luvit.url"
local http = require "luvit.http"
local https = require "luvit.https"
local setTimeout = require "luvit.timer".setTimeout

mq.exchange("mojangFetcher")

local listener = coroutine.create(function()
    while true do
        mq.listen({mq.QUEUE.MOJANG_API}, function(consumer_tag, tag, data)
            local response = coroutine.yield(tag, data)
            data.response = response

            if response then
                mq.publish(mq.QUEUE.GENERAL_PROCESSING, function()
                    print ("Emit callback", data.callbackTag, data.url)
                    mq.write(data.callbackTag, data)
                end)
            end
        end)
        print "Restart listening"
    end
end)

local function doNext(v, tag, data)
    assert(tag == mq.TAG.FETCH_URL, "Invalid tag: " .. tag .. " != " .. mq.TAG.FETCH_URL)

    local url = url.parse(data.url)

    local lib = https

    if url.protocol == "http" then
        lib = http
    end

    -- TODO: retry, log errors, abort if inpossible
    local parser = JSON.streamingParser(function (value)
        return doNext(coroutine.resume(listener, value))
    end)

    local req = lib.request(url, function(res)
        if res.statusCode == 204 then
            print ("Invalid UUID", data.url)
            return doNext(coroutine.resume(listener, nil))
        end

        if res.statusCode == 429 then
            print ("Delay", data.url)
            setTimeout(20000, function()
                doNext(v, tag, data)
            end)
            return
        end

        if res.statusCode == 500 then
            print ("Server Error", data.url)
            setTimeout(20000, function()
                doNext(v, tag, data)
            end)
            return
        end

        assert(res.statusCode == 200, "Invalid status code: " .. res.statusCode .. " for url " .. data.url)

        res:on("data", function(chunk)
            parser:parse(chunk)
        end)

        res:on("end", function()
            parser:complete()
        end)
    end)

    req:done()
end

doNext(coroutine.resume(listener))
