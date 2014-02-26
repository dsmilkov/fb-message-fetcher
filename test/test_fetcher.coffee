fs = require 'fs'
fbchat = require '../lib/fetcher'

accessToken = process.argv[2]

start = process.hrtime()
fbchat.downloadFBMessages accessToken, (err, people, threads) ->
  throw err if err
  end = process.hrtime(start)
  elapsed = end[0]*1000 + end[1] / 1000000 # elapsed time in ms
  console.log "Download all messages in #{elapsed}ms"
  fs.writeFile './threads.json', JSON.stringify(threads, null, 4), (err) ->
    throw err if err
    console.log "Saved message threads on disk"
  fs.writeFile './people.json', JSON.stringify(people, null, 4), (err) ->
    throw err if err
    console.log "Saved people on disk"

  
