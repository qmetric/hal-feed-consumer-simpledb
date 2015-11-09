package com.qmetric.feed.consumer.simpledb

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.simpledb.utils.SimpleDBUtils
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.store.FeedTrackerSpecification
import org.joda.time.DateTime
import spock.lang.Shared

import static com.qmetric.feed.consumer.simpledb.DomainNameFactory.userPrefixedDomainName
import static com.qmetric.feed.consumer.simpledb.utils.TestEnvironment.*

class SimpleDBFeedTrackerAcceptanceTest extends FeedTrackerSpecification {

    def 'throw exception if tries to track the same entry many times - TODO: maybe better idempotent?'() {
        given:
        DateTime timeOfSeenEntry = someTime.minusMinutes(10)
        SeenEntry entryToBeTracked = new SeenEntry(EntryId.of("1"), timeOfSeenEntry)

        when:
        feedTracker.track(entryToBeTracked)
        feedTracker.track(entryToBeTracked)

        then:
        thrown(AmazonServiceException)
        takeOne(feedTracker.getEntriesToBeConsumed()).created == timeOfSeenEntry
    }

    def 'throw exception when tries to track not existing entry - TODO: should be able to mark as consuming without having tracked first'() {
        given:
        !feedTracker.isTracked(seenEntry1.id)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        noExceptionThrown()
    }

    private static final DOMAIN_NAME = userPrefixedDomainName('hal-feed-consumer-test')
    @Shared private AmazonSimpleDBClient simpleDBClient
    @Shared private SimpleDBUtils simpleDBUtils

    def setupSpec() {
        verifyEnvironment()
        simpleDBClient = new SimpleDBClientFactory(accessKey(), secretKey()).simpleDBClient()
        simpleDBUtils = new SimpleDBUtils(simpleDBClient)
    }

    @Override
    protected void afterEachTest() {
        simpleDBUtils.deleteDomain(DOMAIN_NAME)
    }

    @Override
    protected FeedTracker feedTrackedImplementation() {
        new SimpleDBFeedTracker(simpleDBClient, DOMAIN_NAME, dateTimeSource)
    }
}
