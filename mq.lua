local rb = require "rabbitmq"
local os = require "os"
local JSON = require "luvit.json"

local seperator = ("").char(0)

local QUEUE = {
    GENERAL_PROCESSING = "GeneralProcessing",
    MOJANG_API = "MojangRequestAPI",
    AMAZON_S3_API = "AmazonS3API",
    IMAGE_PROCESSING = "ImageProcessing",
    DATABASE_UPDATER = "DatabaseUpdater",
}

local function parseData(raw)
    local idx = raw:find(seperator)
    local data = raw:sub(idx+1)
    pcall(function()
        data = JSON.parse(data)
    end)
    return raw:sub(1, idx-1), data
end

local function packData(tag, data)
    assert(type(tag) == "string")
    return tag .. seperator .. JSON.stringify (data)
end

local conn = rb.connect_rabbit{host="localhost"}

rb.declare_queue(conn, QUEUE.GENERAL_PROCESSING,    { durable = 1 })
rb.declare_queue(conn, QUEUE.MOJANG_API,            { durable = 1 })
rb.declare_queue(conn, QUEUE.AMAZON_S3_API,         { durable = 1 })
rb.declare_queue(conn, QUEUE.IMAGE_PROCESSING,      { durable = 1 })
rb.declare_queue(conn, QUEUE.DATABASE_UPDATER,      { durable = 1 })

local LOCAL_EXCHANGE

return {
    rb = rb,

    QUEUE = QUEUE,

    TAG = {
        RENDER_IMG = "render",

        PRE_FETCH_NAMES = "pre_name",
        PRE_FETCH_PROFILE = "pre_profile",
        PRE_FETCH_ALL = "pre_all",

        FETCH_PROFILE = "profile",
        FETCH_NAMES = "names",

        FETCH_URL = "fetch",
        PROCESS_PROFILE_PROPERTY = "profile_property",

        UPDATE_PROFILE_TIME = "profile_time",

        PROCESS_SKIN = "skin",
        PROCESS_NAMES = "process_names",
    },

    exchange = function(name)
        LOCAL_EXCHANGE = name
        rb.declare_exchange(conn, LOCAL_EXCHANGE, "fanout", { durable = 1 })
    end,

    write = function(tag, data)
        rb.publish(conn, LOCAL_EXCHANGE, packData(tag, data))
    end,

    publish = function(queue, cb)
        local boundQueue = rb.bind_queue(conn, queue, LOCAL_EXCHANGE)
        cb()
        boundQueue()
    end,

    listen = function(queues, cb, count)
        local call = cb
        cb = function(consumer_tag, data)
            call(consumer_tag, parseData(data))
        end

        local opts = {}

        for k, queue in pairs(queues) do
            opts[queue] = { cb }
        end

        rb.wait_for_messages(conn, opts, count)
    end
}
