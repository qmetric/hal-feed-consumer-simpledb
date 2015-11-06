package com.qmetric.feed.consumer.simpledb

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.simpledb.AmazonSimpleDB
import com.amazonaws.services.simpledb.model.*
import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.TrackedEntry
import com.qmetric.feed.consumer.store.AlreadyConsumingException
import com.qmetric.feed.consumer.store.ConnectivityException
import org.joda.time.DateTime
import spock.lang.Specification

import static com.qmetric.feed.consumer.simpledb.utils.TrackedEntryBuilder.trackedEntryBuilder
import static net.java.quickcheck.generator.PrimitiveGeneratorSamples.anyNonEmptyString
import static org.apache.commons.lang3.StringUtils.isBlank

class SimpleDBFeedTrackerTest extends Specification {

    private static FAILURES_COUNT = 'failures_count'

    private static SEEN_AT = 'seen_at'

    private static CONSUMING = "consuming"

    private static final CURRENT_DATE_STRING = "2014/01/10 12:00:00"

    private static final CURRENT_DATE = new DateTime(2014, 1, 10, 12, 0, 0, 0)

    final domain = anyNonEmptyString()

    final feedEntryId = EntryId.of(anyNonEmptyString())

    final simpleDBClient = Mock(AmazonSimpleDB)

    final dateTimeSource = Mock(DateTimeSource.class)

    final consumedEntryStore = new SimpleDBFeedTracker(simpleDBClient, domain, dateTimeSource)

    def setup() {
        dateTimeSource.now() >> CURRENT_DATE
    }

    def "should try to store entry with consuming state"()
    {
        when:
        consumedEntryStore.markAsConsuming(feedEntryId)

        then:
        1 * simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == itemName(feedEntryId)
            assert r.attributes.size() == 1
            r.attributes.get(0).with {
                assert name == CONSUMING
                assert !isBlank(value)
            }
            r.expected.with {
                assert name == CONSUMING
                assert !exists
            }
        }
    }

    def "should finish silently if NO conditional check problems experienced"()
    {
        given: "simpleDBClient puts attributes without any problems"
        simpleDBClient.putAttributes(_) >> {}

        when:
        consumedEntryStore.markAsConsuming(feedEntryId)

        then:
        noExceptionThrown()
    }

    def "should throw AlreadyConsumingException when attempting to set consuming state for entry already being consumed by another consumer"()
    {
        given: "simpleDBClient experienced some problems when putting the attributes"
        simpleDBClient.putAttributes(_) >> {
            final conditionalCheckFailed = new AmazonServiceException("Conditional check failed. Attribute (consuming) value exists")
            conditionalCheckFailed.setErrorCode("ConditionalCheckFailed")
            throw conditionalCheckFailed
        }

        when:
        consumedEntryStore.markAsConsuming(feedEntryId)

        then:
        thrown(AlreadyConsumingException)
    }

    def 'fail should remove "consuming" attribute regardless whether retry is scheduled or not'()
    {
        given:
        final TrackedEntry trackedEntry = trackedEntryBuilder().withEntryId(feedEntryId).build()

        when:
        consumedEntryStore.fail(trackedEntry, scheduleRetry)

        then:
        1 * simpleDBClient.deleteAttributes(_ as DeleteAttributesRequest) >> { DeleteAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntryId.toString()
            assert r.attributes.name == [CONSUMING]
        }

        where:
        scheduleRetry << [true, false]
    }

    def 'fail with scheduledRetry set should increment fail count'()
    {
        given:
        final boolean scheduleRetry = true
        final TrackedEntry trackedEntry = trackedEntryBuilder()
                .withEntryId(feedEntryId)
                .withRetries(initial_count)
                .build()

        when:
        consumedEntryStore.fail(trackedEntry, scheduleRetry)

        then: 'increment failures count and update seen_at date'
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntryId.toString()
            assert it.attributes == [expectedFailureCountAttribute, expectedSeenAtAttribute]
        }

        where:
        initial_count | incremented_count
        0             | 1
        1             | 2
        2             | 3

        expectedFailureCountAttribute = new ReplaceableAttribute(FAILURES_COUNT, incremented_count.toString(), true)
        expectedSeenAtAttribute = new ReplaceableAttribute(SEEN_AT, CURRENT_DATE_STRING, true)
    }

    def 'fail WITHOUT scheduledRetry set should abort further retries'()
    {
        given:
        final boolean scheduleRetry = false
        final TrackedEntry trackedEntry = trackedEntryBuilder().withEntryId(feedEntryId).build()
        final expectedAbortedAttribute = new ReplaceableAttribute("aborted", CURRENT_DATE_STRING, true)

        when:
        consumedEntryStore.fail(trackedEntry, scheduleRetry)

        then:
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest it ->
            assert it.domainName == domain
            assert it.itemName == feedEntryId.toString()
            assert it.attributes == [expectedAbortedAttribute]
        }
    }

    def 'should mark consumed entry with "consumed" attribute'()
    {
        when:
        consumedEntryStore.markAsConsumed(feedEntryId)

        then:
        simpleDBClient.putAttributes(_) >> { PutAttributesRequest r ->
            assert r.domainName == domain
            assert r.itemName == feedEntryId.toString()
            assert r.attributes == [new ReplaceableAttribute().withName("consumed").withValue(CURRENT_DATE_STRING).withReplace(true)]
        }
    }

    def 'should add expected attributes when tracking'()
    {
        given:
        final dateTime = new DateTime(2015, 5, 1, 12, 0, 0, 0)
        final SeenEntry seenEntry = new SeenEntry(feedEntryId, dateTime)

        when:
        consumedEntryStore.track(seenEntry)

        then:
        1 * simpleDBClient.putAttributes(_ as PutAttributesRequest) >> { PutAttributesRequest r ->
            assert r.itemName == feedEntryId.toString()
            assert r.attributes == [
                    new ReplaceableAttribute().withName("created").withValue("2015/05/01 12:00:00").withReplace(false),
                    new ReplaceableAttribute().withName("seen_at").withValue("2015/05/01 12:00:00").withReplace(true)
            ]
        }
    }

    def 'should return whether entry is tracked or not'()
    {
        given:
        simpleDBClient.select(_ as SelectRequest) >> new SelectResult().withItems(returnedItems)

        when:
        final isTracked = consumedEntryStore.isTracked(feedEntryId)

        then:
        isTracked == shouldSayItsTracked

        where:
        returnedItems            | shouldSayItsTracked
        []                       | false
        [new Item()]             | true
        [new Item(), new Item()] | true
    }

    def "should ask simple db client about unconsumed entries"()
    {
        when:
        consumedEntryStore.getEntriesToBeConsumed()

        then:
        1 * simpleDBClient.select(_) >> { SelectRequest r ->
            def whereCondition = r.getSelectExpression().split("(?i)where")[1]
            assert whereCondition.contains('aborted')
            assert whereCondition.contains('consuming')
            assert whereCondition.contains('consumed')
            return new SelectResult().withItems([])
        }
    }

    def "should return entries to be consumed as a collection of TrackedEntry"()
    {
        given:
        def items = [
                new Item().withName("foo").withAttributes(
                        new Attribute("created", CURRENT_DATE_STRING)
                ),
                new Item().withName("bar").withAttributes(
                        new Attribute("failures_count", "2")
                )
        ]
        simpleDBClient.select(_) >> new SelectResult().withItems(items)

        when:
        def notConsumedResult = consumedEntryStore.getEntriesToBeConsumed()

        then:
        notConsumedResult.toList().id == [EntryId.of("foo"), EntryId.of("bar")]
        notConsumedResult.toList().retries == [0 , 2]
        notConsumedResult.toList().created == [CURRENT_DATE, null]
    }

    def "should know when connectivity to store is healthy"()
    {
        when:
        consumedEntryStore.checkConnectivity()

        then:
        notThrown(ConnectivityException)
    }

    def "should know when connectivity to store is unhealthy"()
    {
        given:
        simpleDBClient.domainMetadata(new DomainMetadataRequest(domain)) >> { throw new Exception() }

        when:
        consumedEntryStore.checkConnectivity()

        then:
        thrown(ConnectivityException)
    }

    private static String itemName(final EntryId entry)
    {
        entry.toString()
    }
}