# simple pub/sub protocol.
Use separate ports for pub and sub, this will reduce negotiation messages.
publish port is 7878, subscribe port is 8787.

## For pub:
### single message publication
send a json header followed by the payload (we could use a marker byte
of some sort or just expect the server to count brackets to know when
the header is finished- that is probably most flexible).
header:
```
{
"topic": "TOPIC",
"payload_size": bytes,
"checksum": "sha1 of payload bytes"
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
{"batch_type": "Count", "count": n}
```

The "Count" header should be followed by n messages with the canonical json
header and payload outlined for single message publication.

#### stream oriented
Send the following json header indicating the start of a stream of messages.
```
{"batch_type": "Start"}
```
The "start" header can be followed by any number of messages with the
canonical json header and payload outlined for single message publication.

When finished sendng messages send the following json indicating the end of the
stream.
```
{"batch_type": "End"}
```
(editors note:  json headers are Case Sensitive!)

## pub acks
### single message publication
sever can respond with either:
`{ "status": "OK" }` or
`{ "status": "ERROR", "code": code, "message": "SOME MESSAGE" }`

### multi message publication
sever can respond with either:
`{ "status": "OK", "count": N}` or
`{ "status": "ERROR", "code": code, "message": "SOME MESSAGE" }`

Where count is the number of messages the server received.

## For sub:
client sends handshake that defines topics it wants:
```
{
"topics": ["TOPIC1", "TOPIC2", "..."]
}
```
Server responds with same response for pub (ok/error).

Then server sends client messages:
```
{
"topic": "TOPIC",
"sequence": SEQUENCE_NUMBER,
"payload_size": bytes,
"checksum": "sha1 of payload bytes"
}
PAYLOAD_BYTES
```

SEQUENCE_NUMBER shall be incrementing.  Each message received on a topic shall
increase by one.  (A server may start out at zero, but a late joining client
shouldn't assume this will start at zero.)

Client responds with:
`{ "status": "OK" }` or `{ "status": "CLOSE" }`


## Errors

1. If you are a server, and you receive an error, you may log and ignore it.

2. If you are a client, and you receive an error, you may log and exit.

## Future development of protocol

1. batching messages
2. multiple servers
3. an "earliest" / "latest" concept from kafka's auto.offset.reset
4. instead of sending back an acknowledgement as a requirement, send back an optional acknowledgement with a sequence number

