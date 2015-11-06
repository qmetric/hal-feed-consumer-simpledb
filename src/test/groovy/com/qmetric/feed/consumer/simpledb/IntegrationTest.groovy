package com.qmetric.feed.consumer.simpledb

import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.Item
import com.qmetric.feed.consumer.*
import com.qmetric.feed.consumer.store.FeedTracker
import com.qmetric.feed.consumer.simpledb.utils.MockEntryHandler
import com.qmetric.feed.consumer.simpledb.utils.MockFeedHandler
import com.qmetric.feed.consumer.simpledb.utils.SimpleDBUtils
import org.junit.BeforeClass
import org.junit.Test
import spark.Spark

import static com.qmetric.feed.consumer.simpledb.DomainNameFactory.userPrefixedDomainName
import static com.qmetric.feed.consumer.simpledb.utils.TestEnvironment.*
import static java.util.concurrent.TimeUnit.SECONDS
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.MatcherAssert.assertThat
import static org.mockito.Mockito.*

class IntegrationTest {
    private static final FEED_SIZE = 9
    private static final PAGE_SIZE = 3
    private static final FEED_SERVER_PORT = 15000
    private static final DOMAIN_NAME = userPrefixedDomainName('hal-feed-consumer-test')
    private final AmazonSimpleDBClient simpleDBClient
    private final SimpleDBUtils simpleDBUtils
    private final FeedTracker tracker

    private final ConsumeAction action = mock(ConsumeAction)

    private final FeedConsumerScheduler consumer

    public IntegrationTest()
    {
        verifyEnvironment()
        simpleDBClient = new SimpleDBClientFactory(accessKey(), secretKey()).simpleDBClient()
        simpleDBUtils = new SimpleDBUtils(simpleDBClient)
        tracker = new SimpleDBFeedTracker(simpleDBClient, DOMAIN_NAME)
        consumer = new FeedConsumerConfiguration("test-feed")
                .consumeEachEntryWith(action)
                .withFeedTracker(tracker)
                .pollForNewEntriesEvery(30, SECONDS)
                .fromUrl("http://localhost:${FEED_SERVER_PORT}/feed").build()
    }

    @BeforeClass public static void startupServer()
    {
        Spark.setPort(FEED_SERVER_PORT)
        Spark.get(new MockFeedHandler("/feed", FEED_SIZE, PAGE_SIZE))
        Spark.get(new MockEntryHandler())
    }

    @Test(timeout = 60000L) public void 'all entries provided by the mock feed are stored'()
    {
        when(action.consume(any(FeedEntry.class))).thenReturn(Result.successful())
        // Workaround: @Before and @After methods were not run at the right time on Travis
        simpleDBUtils.createDomainAndWait(DOMAIN_NAME)

        consumer.start()
        waitConsumerToRunOnce(consumer)
        consumer.stop()
        verify(action, times(FEED_SIZE)).consume(any(FeedEntry))
        def result = simpleDBUtils.select("select * from `${DOMAIN_NAME}`")
        assertThat(result.items.size(), equalTo(FEED_SIZE))
        result.items.each { Item it ->
            def attributes = it.attributes.collectEntries { [it.name, it.value] }
            assertThat(attributes.containsKey('consumed'), equalTo(true))
            assertThat(attributes.containsKey('consuming'), equalTo(true))
            assertThat(attributes.containsKey('seen_at'), equalTo(true))
        }

        // Workaround: @Before and @After methods were not run at the right time on Travis
        simpleDBUtils.deleteDomain(DOMAIN_NAME)
    }

    private static void waitConsumerToRunOnce(FeedConsumerScheduler consumer)
    {
        while (consumer.getInvocationsCount() < 1)
        {
            SECONDS.sleep(5)
        }
    }
}
