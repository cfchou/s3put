## S3Put
Upload(HTTP PUT) a file to Amazon S3 using _spray.io_ and S3's REST API.

### Current Status
For demo only.

### Details
There're currently three different actors reside in the package.

#### In cf.s3.obsolete ####
* _S3Put_: very rudimentary. It allows a client to send a file in one go.
* _S3StreamPut_: [S3 doesn't support `Transfer-Encoding: chunked`]
(http://aws.amazon.com/articles/1109#14). However,  _spray_ supports
`chunkless-streaming`. From the client's point of view, it sends a file in
chunks, albeit _spray_ behind the scene sends chunks in one request.

#### In cf.s3 ####
* _S3StreamPutFSM_: functions nearly the same as _S3StreamPut_. But it
uses _Akka_'s _FSM_ DSL to manage states. Moreover, __it is most up-to-date__
and thus has fewer pieces of nonsense.

### Test clients
* _S3Client_: uses _S3Put_.
* _S3StreamClient_: uses _S3StremPutFSM_(can switch to _obsolete.S3StreamPut_
instead). It's a reactive actor in itself.
* _S3SerialClient_: uses _S3StremPutFSM_. Unlike _S3StreamClient_'s reactive
approach, it applies ask pattern and sequences Futures.


