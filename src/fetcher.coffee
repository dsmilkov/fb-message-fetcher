FB = require('fb')
async = require('async')

LIMIT_THREADS = 100
LIMIT_MESSAGES = 5000
THREADS_PER_QUERY = 150

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
  query = "SELECT total_threads FROM unified_thread_count WHERE folder = 'inbox' OR folder = 'other'"
  FB.api 'fql', { q: query }, (res) ->
    return callback new Error res.error.message if not res? or res.error  
    thread_count = 0
    thread_count += thread.total_threads for thread in res.data
    callback null, thread_count

getThreads = (offset, callback) ->
  console.log "making threads fql query with offset " + offset
  query = "SELECT thread_id, thread_fbid, name, num_messages, former_participants, participants, timestamp FROM unified_thread WHERE folder = 'inbox' OR folder = 'other' limit " + LIMIT_THREADS + " offset " + offset
  FB.api 'fql', {q: query}, (res) ->
    return callback new Error res.error.message if not res? or res.error
    callback null, res.data

getAllThreads = (thread_count, callback) ->
  # thread_count is an upper bound for the number of threads
  calls = for i in [0..Math.ceil(thread_count / LIMIT_THREADS)-1]
    do (i) ->
      (callback) -> getThreads i * LIMIT_THREADS, callback 
  async.parallel calls, (err, res) ->
    return callback(err) if err 
    # join the different results
    threads = res.reduce (prev, curr) ->
      prev.concat curr
    , []
    return callback(null, threads)
 
getMessages = (thread_ids, callback) -> 
  thread_ids = ("'" + threadid + "'" for threadid in thread_ids).join ','
  query = "SELECT thread_id, timestamp, sender, body FROM unified_message WHERE thread_id IN (" + thread_ids + ") limit " + LIMIT_MESSAGES + " offset 0"
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
      if bin.indexes.length < THREADS_PER_QUERY and bin.nmessages + thread.num_messages <= LIMIT_MESSAGES
        bin.indexes.push i
        bin.nmessages += thread.num_messages
        found = true
        break
    # start a new bin
    bins.push { nmessages: thread.num_messages, indexes: [i] } if not found  
  bins

bestFit = (threads) ->
  bins = [{ nmessages: 0, indexes: []}]
  for thread, i in threads
    bestbin = null
    min_residual = LIMIT_MESSAGES + 1
    for bin in bins
      if bin.indexes.length < THREADS_PER_QUERY and bin.nmessages + thread.num_messages <= LIMIT_MESSAGES 
        resid = LIMIT_MESSAGES - bin.nmessages - thread.num_messages
        if resid < min_residual 
          min_residual = resid
          bestbin = bin  
    if bestbin?
      bestbin.indexes.push i
      bestbin.nmessages += thread.num_messages
    else
      # start a new bin
      bins.push { nmessages: threads[i].num_messages, indexes: [i] }

exports.downloadFBMessages = (accessToken, callback) ->
  FB.setAccessToken accessToken

  getThreadCount (err, thread_count) ->
    callback(err) if err
    getAllThreads thread_count, (err, threads) ->
      callback(err) if err
      console.log "Downloaded #{threads.length} threads"
      getAllMessages threads, (err, messages) ->
        callback(err) if err
        console.log "Downloaded #{messages.length} messages"
        # find all senders
        people = {}
        threadid2thread = {}
        for thread in threads
          threadid2thread[thread.thread_id] = thread
          for participant in thread.former_participants.concat thread.participants
            people[participant.user_id] = { name: participant.name, email: participant.email } 
          thread.former_participants = (participant.user_id for participant in thread.former_participants)
          thread.participants = (participant.user_id for participant in thread.participants)
          thread.messages = []  
        # combine messages to threads and compress sender info
        for message in messages
          threadid2thread[message.thread_id].messages.push(message)
          message.sender = message.sender.user_id
          delete message['thread_id']
        callback(null, people, threads)