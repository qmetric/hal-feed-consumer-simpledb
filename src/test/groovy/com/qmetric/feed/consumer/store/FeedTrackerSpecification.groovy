package com.qmetric.feed.consumer.store

import com.qmetric.feed.consumer.DateTimeSource
import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.SeenEntry
import com.qmetric.feed.consumer.TrackedEntry
import com.qmetric.feed.consumer.retry.RetryStrategy
import org.joda.time.DateTime
import spock.lang.Specification

import static com.qmetric.feed.consumer.store.TrackedEntryBuilder.trackedEntryBuilder

abstract class FeedTrackerSpecification extends Specification {

    def 'track entries'() {
        given:
        assert !feedTracker.isTracked(seenEntry1.id)
        assert !feedTracker.isTracked(seenEntry2.id)

        when:
        feedTracker.track(seenEntry1)

        then:
        feedTracker.isTracked(seenEntry1.id)
        !feedTracker.isTracked(seenEntry2.id)
    }

    def 'consider tracked entries as ready to be consumed'() {
        given:
        assert count(feedTracker.getEntriesToBeConsumed()) == 0

        when:
        feedTracker.track(seenEntry1)
        feedTracker.track(seenEntry2)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 2
    }

    def 'use information from seen entries to create tracked entries'() {
        when:
        feedTracker.track(seenEntry2)
        feedTracker.track(seenEntry1)

        then:
        Collection<EntryId> storedEntryIds = idsOf(feedTracker.getEntriesToBeConsumed())
        storedEntryIds.size() == 2
        storedEntryIds.containsAll([seenEntry1.id, seenEntry2.id])
    }

    def 'use the time from seen entry to set the creation time'() {
        given:
        DateTime timeOfSeenEntry = someTime.minusMinutes(10)
        DateTime timeWhenProcessingOccurs = someTime.plusMinutes(5)
        SeenEntry entryToBeTracked = new SeenEntry(EntryId.of("1"), timeOfSeenEntry)
        dateTimeSource.now() >> timeWhenProcessingOccurs

        when:
        feedTracker.track(entryToBeTracked)

        then:
        takeOne(feedTracker.getEntriesToBeConsumed()).created == timeOfSeenEntry
    }

    def 'not consider entries already being consumed as available to be consumed again'() {
        given:
        feedTracker.track(seenEntry1)
        feedTracker.track(seenEntry2)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        idsOf(feedTracker.getEntriesToBeConsumed()) == [seenEntry2.id]
    }


    def 'prevent multiple consumers from consuming an entry already being consumed'() {
        given:
        feedTracker.track(seenEntry1)
        feedTracker.markAsConsuming(seenEntry1.id)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        thrown(AlreadyConsumingException)
    }

    def 'still consider a being consumed entry as tracked'() {
        given:
        feedTracker.track(seenEntry1)

        when:
        feedTracker.markAsConsuming(seenEntry1.id)

        then:
        feedTracker.isTracked(seenEntry1.id)
    }

    def 'NOT consider permanently failed entries as available to be consumed'()
    {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        boolean scheduleRetry = false

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 0
    }


    def 'still consider entries failed with potential retry as available to be consumed'()
    {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        boolean scheduleRetry = true

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 1
        takeOne(feedTracker.getEntriesToBeConsumed()).id == trackedEntry.id
    }

    def 'NOT consider consumed entries as available to be consumed again'() {
        given:
        feedTracker.track(seenEntry1)
        assert count(feedTracker.getEntriesToBeConsumed()) == 1

        when:
        feedTracker.markAsConsumed(seenEntry1.id)

        then:
        count(feedTracker.getEntriesToBeConsumed()) == 0
    }

    def 'increase retries count after each failure'() {
        given:
        feedTracker.track(seenEntry1)
        TrackedEntry trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        assert trackedEntry.retries == 0
        boolean scheduleRetry = true

        when:
        feedTracker.fail(trackedEntry, scheduleRetry)
        trackedEntry = takeOne(feedTracker.getEntriesToBeConsumed())
        feedTracker.fail(trackedEntry, scheduleRetry)

        then:
        takeOne(feedTracker.getEntriesToBeConsumed()).retries == 2
    }

    def "prefer older entries over recently tracked or failed ones so that each one has a chance to be process if a limit imposed"()
    {
        given:
        DateTime currentTime = someTime
        dateTimeSource.now() >>> secondsIntervals(currentTime)
        final seenEntryId = trackNewEntry('seenEntry', currentTime.minusSeconds(15))
        final failedEntryId = trackNewEntry('failedEntry', currentTime.minusSeconds(14))
        final entryBeingConsumedId = trackNewEntry('entryBeingConsumed', currentTime.minusSeconds(12))
        final consumedEntryId = trackNewEntry('consumedEntry', currentTime.minusSeconds(11))
        final abortedEntryId = trackNewEntry('abortedEntry', currentTime.minusSeconds(10))

        feedTracker.markAsConsuming(failedEntryId)
        TrackedEntry failedEntry = trackedEntryBuilder().withEntryId(failedEntryId).build()
        feedTracker.fail(failedEntry, true)

        feedTracker.markAsConsuming(abortedEntryId)
        TrackedEntry abortedEntry = trackedEntryBuilder().withEntryId(abortedEntryId).build()
        feedTracker.fail(abortedEntry, false)

        feedTracker.markAsConsuming(consumedEntryId)
        feedTracker.markAsConsumed(consumedEntryId)

        feedTracker.markAsConsuming(entryBeingConsumedId)

        final anotherSeenEntryId = trackNewEntry('anotherSeenEntry', currentTime.plusSeconds(10))

        when:
        final entries = feedTracker.getEntriesToBeConsumed()

        then:
        idsOf(entries) == [seenEntryId, failedEntryId, anotherSeenEntryId]
    }

    def 'convey information about last update in returned TrackedEntry so that it can be used to apply a replay strategy'() {
        given:
        boolean scheduleRetry = true
        dateTimeSource.now() >>> secondsIntervals(someTime)
        TrackedEntry trackedEntry1 = trackedEntryBuilder().withEntryId(EntryId.of("1")).withRetries(1).build()
        TrackedEntry trackedEntry2 = trackedEntryBuilder().withEntryId(EntryId.of("2")).withRetries(1).build()
        TrackedEntry trackedEntry3 = trackedEntryBuilder().withEntryId(EntryId.of("3")).withRetries(1).build()
        TrackedEntry trackedEntry4 = trackedEntryBuilder().withEntryId(EntryId.of("4")).withRetries(1).build()
        TrackedEntry trackedEntry5 = trackedEntryBuilder().withEntryId(EntryId.of("5")).withRetries(1).build()
        feedTracker.track(new SeenEntry(trackedEntry1.id, someTime))
        feedTracker.track(new SeenEntry(trackedEntry2.id, someTime))
        feedTracker.track(new SeenEntry(trackedEntry3.id, someTime))
        feedTracker.track(new SeenEntry(trackedEntry4.id, someTime))
        feedTracker.track(new SeenEntry(trackedEntry5.id, someTime))

        when:
        feedTracker.fail(trackedEntry1, scheduleRetry)
        feedTracker.fail(trackedEntry2, scheduleRetry)
        feedTracker.fail(trackedEntry3, scheduleRetry)
        DateTime timeCheckpoint = dateTimeSource.now()
        feedTracker.fail(trackedEntry4, scheduleRetry)
        feedTracker.fail(trackedEntry5, scheduleRetry)

        then:
        List<TrackedEntry> toRetryInThisRun = feedTracker.getEntriesToBeConsumed().findAll {
            it.canBeProcessed(applyingStrategyOfTakingAllUpdatedBefore(timeCheckpoint), someTime)
        }
        toRetryInThisRun.id == [trackedEntry1.id, trackedEntry2.id, trackedEntry3.id]
    }

    private static RetryStrategy applyingStrategyOfTakingAllUpdatedBefore(final DateTime timeDelimiter) {
        new RetryStrategy() {
            @Override
            boolean canRetry(int retriesSoFar, DateTime seenAt, DateTime currentTime) {
                seenAt.isBefore(timeDelimiter)
            }
        }
    }

    private static List<DateTime> secondsIntervals(DateTime currentTime) {
        (1..100).collect { currentTime.plusSeconds(it) }
    }

    private EntryId trackNewEntry(String id, DateTime entryDateTime)
    {
        EntryId entryId = EntryId.of("${id}-${uniqueString()}")
        feedTracker.track(new SeenEntry(entryId, entryDateTime))

        entryId
    }

    private static String uniqueString() {
        UUID.randomUUID().toString()
    }

    private static int count(Iterable<Object> items) {
        items.iterator().hasNext() ? (int) items.collect{1}.sum() : 0;
    }

    protected static TrackedEntry takeOne(Iterable<TrackedEntry> entries) {
        return entries.iterator().next()
    }

    private static Collection<EntryId> idsOf(Iterable<TrackedEntry> trackedEntries) {
        trackedEntries.collect { TrackedEntry trackedEntry -> trackedEntry.id }
    }

    def setup() {
        beforeEachTest()
        feedTracker = feedTrackedImplementation()
    }

    def cleanup() {
        afterEachTest()
    }

    protected abstract FeedTracker feedTrackedImplementation()
    protected void beforeEachTest() {}
    protected void afterEachTest() {}

    protected SeenEntry seenEntry1 = new SeenEntry(EntryId.of("1"), new DateTime())
    protected SeenEntry seenEntry2 = new SeenEntry(EntryId.of("2"), new DateTime())
    protected DateTimeSource dateTimeSource = Stub(DateTimeSource)
    protected DateTime someTime = new DateTime(2010, 10, 10, 10, 0, 0, 0);
    protected FeedTracker feedTracker
}