Siddhi IO NATS
======================================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-nats/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-nats/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-io-nats.svg)](https://github.com/siddhi-io/siddhi-io-nats/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-io-nats extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that receives and publishes events from and to NATS.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.io.nats/siddhi-io-nats/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.io.nats/siddhi-io-nats">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.10">2.0.10</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.10/#nats-sink">nats</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink">Sink</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">NATS Sink allows users to subscribe to a NATS broker and publish messages.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-io-nats/api/2.0.10/#nats-source">nats</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">Source</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">NATS Source allows users to subscribe to a NATS broker and receive messages. It has the ability to receive all the message types supported by NATS.</p></p></div>

## Dependencies 

Add necessary NATS client jars and related dependencies. 

* java-nats-streaming-2.2.2.jar (Add to <SIDDHI_HOME>/jars)
* jnats-2.6.5.jar (Add to <SIDDHI_HOME>/jars)
* protobuf-java-3.9.1.jar (Add to <SIDDHI_HOME>/bundles)

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.
