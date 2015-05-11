local mq = require "koh.mq"
local gd = require "gd"
local ffi = require "ffi"
local fs = require "luvit.fs"
local base64 = require "base64"
local min = require "math".min
local max = require "math".max
local floor = require "math".floor
local bit = require "bit"

mq.exchange("skinProcessor")

local const = {
    HeadX      = 8,
    HeadY      = 8,
    HeadWidth  = 8,
    HeadHeight = 8,

    HelmX      = 40,

    TorsoX     = 20,
    TorsoY     = 20,
    TorsoWidth  = 8,
    TorsoHeight = 12,

    RightLegX    = 4,
    RightLegY      = 20,
    RightLegWidth  = 4,
    RightLegHeight = 12,

    RightArmX      = 44,
    RightArmY      = 20,
    RightArmWidth  = 4,
    RightArmHeight = 12,
}

local function skinPath(type, uuid)
    return "skin/" .. type .. "/" .. uuid .. ".png"
end

local function fixAlpha(handle)
    if handle ~= nil then
        gd.gdImageSaveAlpha(handle, 1)
        gd.gdImageAlphaBlending (handle, 0)
    end

    return handle
end

local function writeSkin(path, handle)
    local outSize = ffi.new("int [1]", 0)
    local croppedImg = ffi.gc(gd.gdImagePngPtr(handle, outSize), function(ptr)
        gd.gdFree(ptr)
    end)
    fs.writeFileSync(path, ffi.string(croppedImg, outSize[0]))
end

local function scaleUp(handle)
    gd.gdImageSetInterpolationMethod(handle, gd.GD_NEAREST_NEIGHBOUR)
    local newHandle = ffi.gc(gd.gdImageScale(handle, 50 * handle.sx, 50 * handle.sy), function(ptr)
        if ptr ~= nil then
            gd.gdImageDestroy(ptr)
        end
    end)

    return fixAlpha(newHandle)
end

local function crop(handle, limits)
    local newHandle = ffi.gc(gd.gdImageCreateTrueColor(limits.width, limits.height), function(ptr)
        if ptr ~= nil then
            gd.gdImageDestroy(ptr)
        end
    end)

    fixAlpha(newHandle)

    gd.gdImageCopy (newHandle, handle, 0, 0,
        limits.x, limits.y, limits.width, limits.height)

    return newHandle
end

local function flipX(handle)
    local newHandle = ffi.gc(gd.gdImageClone(handle), function(ptr)
        if ptr ~= nil then
            gd.gdImageDestroy(ptr)
        end
    end)

    gd.gdImageFlipHorizontal(newHandle)

    return fixAlpha(newHandle)
end

local function combine(limits, layers)
    local newHandle = ffi.gc(gd.gdImageCreateTrueColor(limits.width, limits.height), function(ptr)
        if ptr ~= nil then
            gd.gdImageDestroy(ptr)
        end
    end)

    fixAlpha(newHandle)

    local h = limits.height
    local w = limits.width
    local pixels = ffi.new("int[?]", w * h, 0x7F000000);

    for _, layer in pairs(layers) do
        local offsetX = 0
        local offsetY = 0

        if type(layer) == "table" then
            offsetX = layer[2]
            offsetY = layer[3]
            layer = layer[1]
        end

        for x = 0, min(layer.sx, limits.width)-1 do
            for y = 0, min(layer.sy, limits.height)-1 do
                local color = gd.gdImageGetTrueColorPixel(layer, x, y)
                local a = bit.rshift(bit.band(color, 0x7F000000), 24)

                if _ == 1 or a == 0 then
                    local x_ = x + offsetX
                    local y_ = y + offsetY
                    pixels[x_ * h + y_] = color
                end
            end
        end
    end

    for i = 0, (w * h) - 1 do
        local y = i % h
        local x = floor(i/h)

        gd.gdImageSetPixel(newHandle, x, y, pixels[i])
    end

    return newHandle
end

local function processSkin(data)
    local img = base64.decode(data.texture)
    local buf = ffi.new('char[?]', #img)
    ffi.copy(buf, img, #img)

    local handle = fixAlpha(ffi.gc(gd.gdImageCreateFromPngPtr(#img, buf), function(ptr)
        if ptr ~= nil then
            gd.gdImageDestroy(ptr)
        end
    end))

    if handle == nil then
        print ("Invalid image format for", data.uuid, data.id)
        return
    end

    writeSkin(skinPath("skin", data.uuid), scaleUp(handle))

    local limits = {}
    limits.x = const.HeadX
    limits.y = const.HeadY
    limits.width = const.HeadWidth
    limits.height = const.HeadHeight

    local headLayer = crop(handle, limits)
    writeSkin(skinPath("head", data.uuid), scaleUp(headLayer))

    limits.x = const.HelmX

    local helmetLayer = crop(handle, limits)
    writeSkin(skinPath("helmet", data.uuid), scaleUp(helmetLayer))

    local head = combine(limits, {
        headLayer,
        helmetLayer
    })
    writeSkin(skinPath("head+helmet", data.uuid), scaleUp(head))

    limits.x = const.TorsoX
    limits.y = const.TorsoY
    limits.width = const.TorsoWidth
    limits.height = const.TorsoHeight

    local torsoLayer = crop(handle, limits)
    writeSkin(skinPath("torso", data.uuid), scaleUp(torsoLayer))

    limits.width = max(const.HeadWidth, const.TorsoWidth)
    limits.height = const.TorsoHeight + const.HeadHeight

    local upperBody = combine(limits, {
        {torsoLayer, 0, const.HeadHeight},
        head
    })
    writeSkin(skinPath("torso+head", data.uuid), scaleUp(upperBody))

    limits.x = const.RightLegX
    limits.y = const.RightLegY
    limits.width = const.RightLegWidth
    limits.height = const.RightLegHeight

    local leftLeg = crop(handle, limits)
    writeSkin(skinPath("leg", data.uuid), scaleUp(leftLeg))

    local rightLeg = flipX(leftLeg)
    writeSkin(skinPath("leg", data.uuid .. "-right"), scaleUp(rightLeg))

    limits.width = max(const.HeadWidth, const.TorsoWidth, 2 * const.RightLegWidth)
    limits.height = const.TorsoHeight + const.HeadHeight + const.RightLegHeight

    local thinBody = combine(limits, {
        {leftLeg, 0, const.HeadHeight + const.TorsoHeight},
        {rightLeg, const.RightLegWidth, const.HeadHeight + const.TorsoHeight},
        {torsoLayer, 0, const.HeadHeight},
        head,
    })
    writeSkin(skinPath("torso+head+legs", data.uuid), scaleUp(thinBody))

    limits.x = const.RightArmX
    limits.y = const.RightArmY
    limits.width = const.RightArmWidth
    limits.height = const.RightArmHeight

    local rightArm = crop(handle, limits)
    writeSkin(skinPath("arm", data.uuid), scaleUp(rightArm))

    local leftArm = flipX(rightArm)
    writeSkin(skinPath("arm", data.uuid .. "-right"), scaleUp(leftArm))

    limits.width = max(const.HeadWidth, const.TorsoWidth + 2 * const.RightArmWidth, 2 * const.RightLegWidth)
    limits.height = max(const.TorsoHeight + const.HeadHeight + const.RightLegHeight, const.RightArmHeight)

    local thinBody = combine(limits, {
        {leftLeg, const.RightArmWidth, const.HeadHeight + const.TorsoHeight},
        {rightLeg, const.RightArmWidth + const.RightLegWidth, const.HeadHeight + const.TorsoHeight},
        {leftArm, 0, const.HeadHeight},
        {rightArm, const.RightArmWidth + const.TorsoWidth, const.HeadHeight},
        {torsoLayer, const.RightArmWidth, const.HeadHeight},
        {head, const.RightArmWidth, 0}
    })
    writeSkin(skinPath("body", data.uuid), scaleUp(thinBody))

    print ("Rendered profile picture for:" , data.uuid, data.id)
end

mq.listen({mq.QUEUE.IMAGE_PROCESSING}, function(consumer_tag, tag, data)
    assert(tag == mq.TAG.RENDER_IMG, "Message with invalid tag: " .. tag)
    if data.texture == nil then
        print ("Missing texture to render", data.uuid, data.id, data.url, data.layer)
    else
        processSkin(data)
    end
end, 50)
