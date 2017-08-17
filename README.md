HAL+JSON feed consumer (SimpleDB)
======================

[![Build Status](https://travis-ci.org/qmetric/hal-feed-consumer-simpledb.png)](https://travis-ci.org/qmetric/hal-feed-consumer-simpledb)

SimpleDB feed tracker extension library for use with [hal-feed-consumer](https://github.com/qmetric/hal-feed-consumer).


Usage
-----

[TODO] Add the following dependencies to your project:

```
<dependency>
    <groupId>com.qmetric</groupId>
    <artifactId>hal-feed-consumer</artifactId>
    <version>${3.16 >= VERSION < 5.0}</version>
</dependency>

<dependency>
    <groupId>com.qmetric</groupId>
    <artifactId>hal-feed-consumer-simpledb</artifactId>
    <version>${VERSION}</version>
</dependency>
```


Features
---------

* Processes feed entries
* Supports multiple consumers - refer to [Competing consumer pattern](#competing-consumer-pattern)
* Pre-configured health checks and metrics provided


Usage
-----

First, configure a data store used by the consumer to track which feed entries have already been consumed.
An [Amazon SimpleDB](http://aws.amazon.com/simpledb/) based implementation is supplied as part of this library:

```java
final AmazonSimpleDB simpleDBClient = new AmazonSimpleDBClient(new BasicAWSCredentials("access key", "secret key"));
simpleDBClient.setRegion(getRegion(EU_WEST_1));

final FeedTracker feedTracker = new SimpleDBFeedTracker(simpleDBClient, "your-sdb-domain");
```

Then, build and start a feed consumer:

```java
final FeedConsumerConfiguration feedConsumerConfiguration = new FeedConsumerConfiguration("test-feed")
                .fromUrl("http://your-feed-endpoint")
                .withFeedTracker(feedTracker)
                .consumeEachEntryWith(new ConsumeAction() {
                                          @Override public Result consume(final FeedEntry feedEntry) {
                                              System.out.println("write your code here to consume the next feed entry...");
                                              return Result.successful();
                                      }})
                .pollForNewEntriesEvery(5, MINUTES);

feedConsumerConfiguration.build().start()
```


Competing consumer pattern
--------------------------

Supports the competing consumer pattern. Multiple consumers can read and process entries safely from the same feed.

Note: In order to allow concurrency between multiple consumers, feed entries may be processed in an order differing from their publish date.
