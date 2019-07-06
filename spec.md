# simple pub/sub protocol.
Use a single port for pub and sub, default is 7878.

## For both:
### client identification
Send a json message with client information as first thing.
```
{"Connect": {
    "client_name": "Name of client",
    "group_id": "group id of client"
}}
```
Client name is primarily for informortation while group_id will be used to save
committed offsets.  Server will respond with a status:
`{"Status": { "status": "OK" }}` or
`{"Status": { "status": "ERROR", "code": code, "message": "SOME MESSAGE" }}`

## For pub:
### single message publication
send a json header followed by the payload (we could use a marker byte
of some sort or just expect the server to count brackets to know when
the header is finished- that is probably most flexible).
header:
```
{"Publish":
{
"topic": "TOPIC",
"payload_size": bytes,
"checksum": "sha1 of payload bytes"
}
}
PAYLOAD_BYTES
```

PAYLOAD_BYTES shall be treated as binary data.

(editors note:  more than one publisher is allowed to publish to one topic)
### multi message publication
#### count oriented
Send the following json header indicating the start of a series of message publications
along with a count so the server knows how many messages to expect.
```
{"Batch": {"batch_type": "Count", "count": n}}
```

The "Count" header should be followed by n messages with the canonical json
header and payload outlined for single message publication.

#### stream oriented
Send the following json header indicating the start of a stream of messages.
```
{"Batch": {"batch_type": "Start"}}
```
The "start" header can be followed by any number of messages with the
canonical json header and payload outlined for single message publication.

When finished sendng messages send the following json indicating the end of the
stream.
```
{"Batch": {"batch_type": "End"}}
```
(editors note:  json headers are Case Sensitive!)

## pub acks
### single message publication
sever can respond with either:
`{"Status": { "status": "OK" }}` or
`{"Status": { "status": "ERROR", "code": code, "message": "SOME MESSAGE" }}`

### multi message publication
sever can respond with either:
`{"Status": { "status": "OK", "count": N}}` or
`{"Status": { "status": "ERROR", "code": code, "message": "SOME MESSAGE" }}`

Where count is the number of messages the server received.

## For sub:
Client subscribes to topics it wants with subscribe messages:
```
{"Subscribe":
{
"topic": "TOPIC",
"position": "Earliest | Latest | Current | Offset",
"offset": offset
}
}
```
Only send offset if position is "Offset".  "Current" will use the last committed
offset.
Server responds with same response for pub (ok/error).

Then server sends client messages:
```
{"Message":
{
"topic": "TOPIC",
"sequence": SEQUENCE_NUMBER,
"payload_size": bytes,
"checksum": "sha1 of payload bytes"
}
}
PAYLOAD_BYTES
```

SEQUENCE_NUMBER shall be incrementing.  Each message received on a topic shall
increase by one.  (A server may start out at zero, but a late joining client
shouldn't assume this will start at zero.)

Client can also unsubscribe to a topic with:
```
{"Unsubscribe":
{
"topic": "TOPIC",
}
}
```
Server responds with same response for pub (ok/error).

Client can commit the latest offset it has seen so on reconnect it will get
messages after the commit offset:
```
{"Commit":
{
    "topic": "topic",
    "partition": partition_number, // future use
    "commit_offset": offset to record for this client/topic
}
}
```
Server responds with a commit ack:
```
{"CommitAck":
{
    "topic": "topic",
    "partition": partition_number, // future use
    "offset": offset was committed
}
}
```


Client responds with:
`{ "status": "CLOSE" }` when it wishes to disconnect


## Errors

1. If you are a server, and you receive an error, you may log and ignore it.

2. If you are a client, and you receive an error, you may log and exit.

## Future development of protocol

1. batching messages
2. multiple servers
3. an "earliest" / "latest" concept from kafka's auto.offset.reset
4. instead of sending back an acknowledgement as a requirement, send back an optional acknowledgement with a sequence number

