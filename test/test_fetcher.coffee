fs = require 'fs'
require 'coffee-script/register'
fbchat = require '../src/fetcher'

accessToken = process.argv[2]

start = process.hrtime()
fbchat.downloadFBMessages accessToken, (err, result) ->
  throw err if err
  end = process.hrtime(start)
  elapsed = end[0]*1000 + end[1] / 1000000 # elapsed time in ms
  console.log "Download all messages in #{elapsed}ms"
  fs.writeFile './result.json', JSON.stringify(result, null, 4), (err) ->
    throw err if err
    console.log "Saved result on disk"
  
