package com.qmetric.feed.consumer.simpledb.utils

import com.amazonaws.services.simpledb.AmazonSimpleDBClient
import com.amazonaws.services.simpledb.model.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import static java.util.concurrent.TimeUnit.SECONDS
import static junit.framework.Assert.fail

public class SimpleDBUtils
{

    private static final Logger LOG = LoggerFactory.getLogger(SimpleDBUtils)
    private final AmazonSimpleDBClient client
    private static final MAX_RETRY = 100

    public SimpleDBUtils(AmazonSimpleDBClient client)
    {
        this.client = client
    }

    public void createDomainAndWait(final String domainName)
    {
        createDomain(domainName)
        if (!isDomainCreated(domainName))
        {
            fail("Exceeded domain creation timeout")
        }
    }

    private void createDomain(String domainName)
    {
        LOG.info "Creating domain [${domainName}]"
        client.createDomain(new CreateDomainRequest(domainName))
    }

    private boolean isDomainCreated(String domainName)
    {
        boolean domainCreated = false
        int count = 0
        while (!domainCreated && count < MAX_RETRY)
        {
            try
            {
                client.domainMetadata(new DomainMetadataRequest(domainName))
                domainCreated = true
                LOG.info "Domain [${domainName}] created"
            }
            catch (Exception e)
            {
                count++
                LOG.info "${count} waiting for domain [${domainName}] to be available"
                SECONDS.sleep(10)
            }
        }
        domainCreated
    }


    public void deleteDomain(final String domainName)
    {
        LOG.info "Deleting domain [${domainName}]"
        client.deleteDomain(new DeleteDomainRequest(domainName))
    }

    public SelectResult select(String query)
    {
        return client.select(new SelectRequest(query, true))
    }
}