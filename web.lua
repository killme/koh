local http = require "luvit.http"
local json = require "luvit.json"
local ibmt = require "ibmt"
local odbxuv = require "odbxuv"

local sql = odbxuv.Connection:new()

sql:connect (dofile("database.conf"), function(err, ...)
    if err then error(err) end
    p("CONNECTION OPEN!", ..., sql)
end)

local server
server = http.createServer(function (req, res)
    res:writeHead(200, {['Content-Type'] = 'text/html'})

    res:write([[
        <!DOCTYPE html>
        <html><body>
        <textarea></textarea>
        <button>button</button>
        <script src="http://code.jquery.com/jquery-1.10.2.min.js"></script>
        <script type="text/javascript">
            jQuery(function($) {
                var text = $("textarea");
                var button = $("button");

                var containsAny = function(str, elem) {
                    str = str.toLowerCase();
                    for(var i in elem) {
                        var x = elem[i].toLowerCase();
                        if(str.indexOf(x) > 0) {
                            return true;
                        }
                    }

                    return false;
                }

                var updateFilter = function() {
                    button.text("Hold on ...")
                    var needle = text.val().trim();
                    var empty = needle == "";
                    var needles = needle.split("\n")

                    for(var i = 0; i < needles.size; i ++) {
                        needles[i] = needles[i].trim();
                    }

                    $("img").each(function(k, img) {
                        var value = empty || containsAny(img.title, needles);
                        $(img).toggle(value);
                    })
                    button.text("Imma search LMAO")
                }

                text.on('change paste', updateFilter)
                button.on('click', updateFilter)

                updateFilter()
            })
        </script>
        ]])

    sql:query([[
    SELECT P.id, uuid, name
    FROM player P, player_name N
    WHERE P.id = N.player_id
    ORDER BY P.id
    ]], function(err, q)
        if err then
            error(err)
        end

        q:on("fetch", function() end)

        q:on("row", function(id, uuid, name)
            if tonumber(id) == 881 then
                print ("X", id, uuid, name)
            end
            res:write("<img style=\"width:50px;\" title=\"".. id .. ": " .. uuid .. " - " .. name .."\" src=\"http://hidden-kingdom.us.to/head/" .. uuid .. ".png\" />")
        end)

        q:on("fetched", function()
            res:finish([[</body></html>]])
            print "Finished request"
            q:close(function() end)
        end)

        q:fetch()
    end)


end):listen(8123)
