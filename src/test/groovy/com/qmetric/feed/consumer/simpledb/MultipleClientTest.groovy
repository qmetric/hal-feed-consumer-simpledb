package com.qmetric.feed.consumer.simpledb

import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Optional
import com.qmetric.feed.consumer.*
import com.qmetric.feed.consumer.simpledb.utils.SimpleDBUtils
import com.qmetric.hal.reader.HalResource
import com.theoryinpractise.halbuilder.AbstractRepresentationFactory
import com.theoryinpractise.halbuilder.impl.representations.MutableRepresentation
import org.joda.time.DateTime
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import java.util.concurrent.Executors
import java.util.concurrent.Future


import static com.qmetric.feed.consumer.simpledb.utils.TestEnvironment.*
import static java.lang.Thread.currentThread
import static java.util.Collections.emptyList
import static java.util.concurrent.TimeUnit.MINUTES
import static java.util.concurrent.TimeUnit.SECONDS
import static org.hamcrest.core.IsEqual.equalTo
import static org.junit.Assert.assertThat
import static org.mockito.Mockito.mock

class MultipleClientTest {
    private static final FEED_SIZE = 9
    private static final String FAILING_ENTRY_ID = '1'
    private static AmazonSimpleDBClient client
    private static SimpleDBFeedTracker tracker
    private static final String DOMAIN_NAME = DomainNameFactory.userPrefixedDomainName('hal-feed-consumer-test')
    private static final executor = Executors.newFixedThreadPool(2)
    private static final resolver = new ResourceResolver() {
        @Override Optional<HalResource> resolve(final EntryId id)
        {
            def representation = new HalResource(new ObjectMapper(), new MutableRepresentation(mock(AbstractRepresentationFactory), "/${id.toString()}"))
            return Optional.of(representation)
        }
    }

    @BeforeClass public static void setupFeedTracker()
    {
        verifyEnvironment()
        client = new SimpleDBClientFactory(accessKey(), secretKey()).simpleDBClient()
        tracker = new SimpleDBFeedTracker(client, DOMAIN_NAME)
    }

    @Before public void setupDomain()
    {
        createDomain()
        populateDomain()
    }

    @After public void deleteDomain()
    {
        new SimpleDBUtils(client).deleteDomain(DOMAIN_NAME)
    }

    @Test public void 'a consumer picks up entries _not on the last page_ which another consumer previously failed to process'()
    {
        def slowActionThread = runConsumerInOwnThreadWith(slowFaultyAction)

        println "Test waiting for ${slowFaultyAction} to pick up the first feed entry, before it fails and reverts it"
        SECONDS.sleep(3)
        assertThat("${slowFaultyAction} is still consuming the first entry", tracker.countConsuming(), equalTo(1))
        assertThat("${slowFaultyAction} hasn't finished consuming the first entry", tracker.countConsumed(), equalTo(0))

        def fastActionThread = runConsumerInOwnThreadWith(quickRunningAction)
        println "Test waiting both consumers to complete"
        waitToComplete(fastActionThread, slowActionThread)

        assertThat("${slowFaultyAction} reverted consuming the first entry", tracker.countConsuming(), equalTo(FEED_SIZE - 1))
        assertThat("all other entries have been consumed", tracker.countConsumed(), equalTo(FEED_SIZE - 1))

        println "Test running ${quickRunningAction} again to pick up entries that ${slowFaultyAction} failed to process"
        newConsumer(quickRunningAction).consume()

        assertThat("all entries have been consumed", tracker.countConsumed(), equalTo(FEED_SIZE))
    }

    private static Future<?> runConsumerInOwnThreadWith(ConsumeAction runnable)
    {
        executor.submit(newRunnableConsumerWith(runnable))
    }

    private static void waitToComplete(Future... futures)
    {
        while (futures.any { f -> !f.done })
        {
            println "Spock is waiting for consumers to terminate"
            SECONDS.sleep 2
        }
    }

    private static Runnable newRunnableConsumerWith(ConsumeAction action)
    {
        newRunnable(newConsumer(action))
    }

    private static FeedConsumer newConsumer(ConsumeAction action)
    {
        def entryConsumer = new EntryConsumerImpl(tracker, action, resolver, emptyList(), Optional.absent(), new Interval(15, MINUTES), new DateTimeSource())
        new FeedConsumerImpl(entryConsumer, tracker, emptyList())
    }

    private static Runnable newRunnable(FeedConsumer consumer)
    {
        new Runnable() {
            @Override void run()
            {
                println "Running ${consumer} in thread ${currentThread().name}"
                consumer.consume()
            }
        }
    }

    private static void populateDomain()
    {
        (1..FEED_SIZE).each { int it ->
            def entry = EntryId.of(it == 1 ? FAILING_ENTRY_ID : "${it}")
            tracker.track(new SeenEntry(entry, DateTime.now()))
        }
    }

    private static void createDomain()
    {
        new SimpleDBUtils(client).createDomainAndWait(DOMAIN_NAME)
    }

    private static slowFaultyAction = new ConsumeAction() {
        @Override Result consume(final FeedEntry input)
        {
            if (input.content.resourceLink.get().href.contains(FAILING_ENTRY_ID))
            {
                println "hang-and-fail-action waiting 10 sec before failing on ${input.content.getResourceLink()}"
                SECONDS.sleep(10)
                throw new RuntimeException()
            }
            else
            {
                println "hang-and-fail-action consumed ${input.content.getResourceLink()}"
            }

            Result.successful()
        }
    }

    private static quickRunningAction = new ConsumeAction() {
        @Override Result consume(final FeedEntry input)
        {
            println "quick-action consumed ${input.content.getResourceLink()}"
            Result.successful()
        }
    }
}
