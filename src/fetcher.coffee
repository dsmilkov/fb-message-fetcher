FB = require 'fb'
async = require 'async'

LIMIT_THREADS = 50
LIMIT_MESSAGES = 5000
THREADS_PER_QUERY = 150
LIMIT_PEOPLE = 1000
#
# Randomize array element order in-place.
#  Using Fisher-Yates shuffle algorithm.
# 
shuffleArray = (array) ->
  for i in [array.length-1..1]
    j = Math.floor Math.random() * (i + 1)
    # swap variables
    [array[i], array[j]] = [array[j], array[i]]

getThreadCount = (callback) ->
  query = "SELECT folder_id, total_count FROM mailbox_folder WHERE " + 
    "folder_id = 0"
  FB.api 'fql', { q: query }, (res) ->
    return callback new Error res.error.message if not res? or res.error  
    callback null, res.data

getThreads = (offset, folder_id, callback) ->
  fields = ["thread_id",
            "message_count",
            "recipients"
  ]
  query = "SELECT " + fields.join(',') + " FROM thread WHERE " +
    "folder_id = " + folder_id + " limit " + LIMIT_THREADS + " offset " + offset
  FB.api 'fql', {q: query}, (res) ->
    return callback new Error res.error.message if not res? or res.error
    callback null, res.data

getAllThreads = (folder_count, callback) ->
  allcalls = []
  for folder in folder_count
    thread_count = parseInt folder.total_count
    # thread_count is an upper bound for the number of threads
    calls = for i in [0..Math.ceil(thread_count / LIMIT_THREADS)-1]
      do (i, folder) ->
        (callback) -> getThreads i * LIMIT_THREADS, folder.folder_id, callback
    allcalls = allcalls.concat calls
  async.parallel allcalls, (err, res) ->
    return callback(err) if err 
    # join the different results
    threads = res.reduce (prev, curr) ->
      prev.concat curr
    , []
    return callback null, threads
 
getPeople = (uids, callback) ->
  fields = ["uid", "name"]
  query = 'SELECT ' + fields.join(',') + ' FROM user WHERE uid in (' + uids.join(',') + ') limit 5000 offset 0'
  FB.api 'fql', { q: query }, (res) ->
    return callback new Error res.error.message if not res? or res.error  
    callback null, res.data

getAllPeople = (uids, callback) ->
  calls = for i in [0...Math.ceil(uids.length / LIMIT_PEOPLE)]
    do (i) ->
      (callback) -> getPeople uids[i * LIMIT_PEOPLE ... (i+1)*LIMIT_PEOPLE], callback
  async.parallel calls, (err, res) ->
    return callback err if err 
    # join the different results
    people = res.reduce (prev, curr) ->
      prev.concat curr
    , []
    return callback null, people

getMessages = (thread_ids, callback) -> 
  thread_ids = ("'" + threadid + "'" for threadid in thread_ids).join ','
  fields = ['thread_id',
            'created_time',
            'author_id',
            'body'
  ]
  query = "SELECT " + fields.join(',') + " FROM message WHERE " + 
    "thread_id IN (" + thread_ids + ") limit " + LIMIT_MESSAGES + " offset 0"
  FB.api 'fql', { q: query }, (res) ->
    return callback new Error res.error.message if not res? or res.error  
    callback null, res.data

getAllMessages = (threads, callback) ->
  shuffleArray threads
  bins = firstFit threads 
  calls = for bin in bins
    do (bin) ->
      (callback) ->
        thread_ids = (threads[index].thread_id for index in bin.indexes)
        getMessages thread_ids, callback  
  async.parallel calls, (err, res) ->
    return callback(err) if err
    # join the different results
    messages = res.reduce (prev, curr) ->
      prev.concat curr
    , []
    callback(null, messages)
  
firstFit = (threads) ->
  bins = [{ nmessages: 0, indexes: []}]
  found = false
  for thread, i in threads
    found = false
    for bin in bins
      if bin.indexes.length < THREADS_PER_QUERY and bin.nmessages + thread.message_count <= LIMIT_MESSAGES
        bin.indexes.push i
        bin.nmessages += thread.message_count
        found = true
        break
    # start a new bin
    bins.push { nmessages: thread.message_count, indexes: [i] } if not found  
  bins

bestFit = (threads) ->
  bins = [{ nmessages: 0, indexes: []}]
  for thread, i in threads
    bestbin = null
    min_residual = LIMIT_MESSAGES + 1
    for bin in bins
      if bin.indexes.length < THREADS_PER_QUERY and bin.nmessages + thread.message_count <= LIMIT_MESSAGES 
        resid = LIMIT_MESSAGES - bin.nmessages - thread.message_count
        if resid < min_residual 
          min_residual = resid
          bestbin = bin  
    if bestbin?
      bestbin.indexes.push i
      bestbin.nmessages += thread.message_count
    else
      # start a new bin
      bins.push { nmessages: threads[i].message_count, indexes: [i] }

exports.downloadFBMessages = (accessToken, callback) ->
  FB.setAccessToken accessToken

  getThreadCount (err, folder_count) ->
    callback err if err
    getAllThreads folder_count, (err, threads) ->
      callback err if err
      #console.log "Downloaded #{threads.length} threads"
      getAllMessages threads, (err, messages) ->
        callback err if err
        #console.log "Downloaded #{messages.length} messages"
        # find all senders
        people = {}
        threadid2thread = {}
        for thread in threads
          threadid2thread[thread.thread_id] = thread
          for user_id in thread.recipients
            people[user_id] = {}
          thread.messages = []  
        # combine messages to threads and compress sender info
        for message in messages
          threadid2thread[message.thread_id].messages.push(message)
          delete message['thread_id']
        getAllPeople Object.keys(people), (err, peopleInfo) ->
          callback err if err
          for personInfo in peopleInfo
            people[personInfo.uid] = { name: personInfo.name }
          callback null, { people: people, threads: threads }