local mq = require "koh.mq"
local base64 = require "luautil.base64"
local JSON = require "luvit.json"

mq.exchange("generalProcessor")

mq.listen({mq.QUEUE.GENERAL_PROCESSING}, function(consumer_tag, tag, data)
    if tag == mq.TAG.PRE_FETCH_ALL or tag == mq.TAG.PRE_FETCH_PROFILE then
        mq.publish(mq.QUEUE.MOJANG_API, function()
            print ("Enqueue profile", data.uuid, data.id)
            mq.write(mq.TAG.FETCH_URL, {
                url = "https://sessionserver.mojang.com/session/minecraft/profile/" .. data.uuid:gsub("-", ""),
                uuid = data.uuid,
                id = data.id,
                callbackTag = mq.TAG.FETCH_PROFILE
            })
        end)
    end

    if tag == mq.TAG.PRE_FETCH_ALL or tag == mq.TAG.PRE_FETCH_NAMES then
        mq.publish(mq.QUEUE.MOJANG_API, function()
            print ("Enqueue names", data.uuid, data.id)
            mq.write(mq.TAG.FETCH_URL, {
                url = "https://api.mojang.com/user/profiles/" .. data.uuid:gsub("-", "") .. "/names",
                uuid = data.uuid,
                id = data.id,
                callbackTag = mq.TAG.FETCH_NAMES
            })
        end)
    end

    if  tag == mq.TAG.PRE_FETCH_ALL or
        tag == mq.TAG.PRE_FETCH_PROFILE or
        tag == mq.TAG.PRE_FETCH_NAMES then
        return
    end

    if tag == mq.TAG.FETCH_PROFILE then
        if not data.response then
            print ("Ignoring empty response", data.url, data.id, data.uuid)
            return
        end
        assert(data.response.properties, "Invalid profile response")
        mq.publish(mq.QUEUE.GENERAL_PROCESSING, function()
            for _, property in pairs(data.response.properties or {}) do
                property.decodedValue = base64.decode(property.value or "")
                if property.name == "textures" then
                    mq.write(mq.TAG.PROCESS_PROFILE_PROPERTY, {
                        id = data.id,
                        uuid = data.uuid,
                        name = property.name,
                        value = property.decodedValue
                    })
                    print ("Fetch property:", data.id, data.uuid, property.name, property.decodedValue)
                else
                    print ("unkown player property", property.name)
                end
            end
        end)

        print ("Fetched profile", data.uuid, data.id)
        mq.publish(mq.QUEUE.DATABASE_UPDATER, function()
            mq.write(mq.TAG.UPDATE_PROFILE_TIME, {
                id = data.id,
                uuid = data.uuid
            })
        end)
    elseif tag == mq.TAG.FETCH_NAMES then
        if not data.response then
            print ("Ignoring empty response", data.url, data.id, data.uuid)
            return
        end
        assert(#data.response > 0, "No names were returned for " .. data.uuid)
        mq.publish(mq.QUEUE.DATABASE_UPDATER, function()
            mq.write(mq.TAG.PROCESS_NAMES, {
                id = data.id,
                uuid = data.uuid,
                names = data.response
            })
        end)
    elseif tag == mq.TAG.PROCESS_PROFILE_PROPERTY then
        if data.name == "textures" then
            local value = JSON.parse(data.value)

            print ("Enqueue skin", data.uuid, data.id)
            mq.publish(mq.QUEUE.AMAZON_S3_API, function()
                for layer, texture in pairs(value.textures) do
                    mq.write(mq.TAG.FETCH_URL, {
                        id = data.id,
                        uuid = data.uuid,
                        url = texture.url,
                        layer = layer,
                        texture = texture,
                        callbackTag = mq.TAG.PROCESS_SKIN
                    })
                end
            end)
        else
            error ("Unkown property: " .. data.name)
        end
    elseif tag == mq.TAG.PROCESS_SKIN then
        print ("Process skin", data.uuid, data.id)
        mq.publish(mq.QUEUE.DATABASE_UPDATER, function()
            mq.write(mq.TAG.PROCESS_SKIN, {
                id = data.id,
                uuid = data.uuid,
                layer = data.layer,
                url = data.url,
                texture = data.response
            })
        end)

        if data.layer == "SKIN" then
            assert(data.response, "Missing response")
            mq.publish(mq.QUEUE.IMAGE_PROCESSING, function()
                mq.write(mq.TAG.RENDER_IMG, {
                    id = data.id,
                    uuid = data.uuid,
                    layer = data.layer,
                    url = data.url,
                    texture = data.response
                })
            end)
        else
            print ("Layer not rendered", data.layer)
        end
    else
        p(tag, data)
        print ("Message with invalid tag: " .. tag)
    end
end)
