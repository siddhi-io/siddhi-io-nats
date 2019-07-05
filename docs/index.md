Siddhi IO NATS
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-nats/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-nats/)
  [![GitHub (pre-)Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-nats/all.svg)](https://github.com/siddhi-io/siddhi-io-nats/releases)
  [![GitHub (Pre-)Release Date](https://img.shields.io/github/release-date-pre/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-nats extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events via NATS and HTTPS transports, calls external services, and serves incoming requests and provide synchronous responses.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 5.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.nats/siddhi-io-nats/">here</a>.
* Versions 4.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.execution.string/siddhi-io-nats">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8">2.0.8</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-sink">nats</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>This extension publish the NATS events in any NATS method  POST, GET, PUT, DELETE  via NATS or https protocols. As the additional features this component can provide basic authentication as well as user can publish events using custom client truststore files when publishing events via https protocol. And also user can add any number of headers including HTTP_METHOD header for each event dynamically.<br>Following content types will be set by default according to the type of sink mapper used.<br>You can override them by setting the new content types in headers.<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- TEXT : text/plain<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- XML : application/xml<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- JSON : application/json<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- KEYVALUE : application/x-www-form-urlencoded</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-request-sink">nats-request</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>This extension publish the NATS events in any NATS method  POST, GET, PUT, DELETE  via NATS or https protocols. As the additional features this component can provide basic authentication as well as user can publish events using custom client truststore files when publishing events via https protocol. And also user can add any number of headers including HTTP_METHOD header for each event dynamically.<br>Following content types will be set by default according to the type of sink mapper used.<br>You can override them by setting the new content types in headers.<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- TEXT : text/plain<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- XML : application/xml<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- JSON : application/json<br>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;- KEYVALUE : application/x-www-form-urlencoded<br><br>NATS request sink is correlated with the The NATS reponse source, through a unique <code>sink.id</code>.It sends the request to the defined url and the response is received by the response source which has the same 'sink.id'.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-response-sink">nats-response</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#sink">(Sink)</a>*<br><div style="padding-left: 1em;"><p>NATS response sink is correlated with the The NATS request source, through a unique <code>source.id</code>, and it send a response to the NATS request source having the same <code>source.id</code>. The response message can be formatted in <code>text</code>, <code>XML</code> or <code>JSON</code> and can be sent with appropriate headers.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-source">nats</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The NATS source receives POST requests via NATS or HTTPS in format such as <code>text</code>, <code>XML</code> and <code>JSON</code>. In WSO2 SP, if required, you can enable basic authentication to ensure that events are received only from users who are authorized to access the service.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-request-source">nats-request</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The NATS request is correlated with the NATS response sink, through a unique <code>source.id</code>, and for each POST requests it receives via NATS or HTTPS in format such as <code>text</code>, <code>XML</code> and <code>JSON</code> it sends the response via the NATS response sink. The individual request and response messages are correlated at the sink using the <code>message.id</code> of the events. If required, you can enable basic authentication at the source to ensure that events are received only from users who are authorized to access the service.</p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.8/#nats-response-source">nats-response</a> *<a target="_blank" href="https://siddhi.io/en/v5.0/docs/query-guide/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The nats-response source co-relates with nats-request sink  with the parameter 'sink.id'.<br>This receives responses for the requests sent by the nats-request sink which has the same sink id.<br>Response messages can be in formats such as TEXT, JSON and XML.<br>In order to handle the responses with different nats status codes, user is allowed to defined the acceptable response source code using the parameter 'nats.status.code'<br></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
