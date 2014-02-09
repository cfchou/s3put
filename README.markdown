## S3Put
Upload(HTTP PUT) a file to Amazon S3 using _spray.io_ and S3's REST API.

## Current Status
For demo only.

## Details
There're currently three different actors reside in the package.

* _S3Put.scala_: most rudimentary. It allows a client to send a file in one go.
See S3PutClient.scala.
* _S3StreamPut.scala_: [S3 doesn't support `Transfer-Encoding: chunked`]
(http://aws.amazon.com/articles/1109#14). However,  _spray_ supports
`chunkless-streaming`. From the client's view, it sends a file in chunks, albeit
it assembles and still sends chunks in one request.
* _S3StreamPutFSM.scala_: same as _S3StreamPut.scala_. But it uses _Akka_'s _FSM_
DSL to manage states.

_S3StreamPutFSM.scala_ is the most up-to-date implementation.


