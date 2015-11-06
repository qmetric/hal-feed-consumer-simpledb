package com.qmetric.feed.consumer.simpledb

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.simpledb.AmazonSimpleDBClient

import static com.amazonaws.regions.Region.getRegion

class SimpleDBClientFactory {

    private final String accessKey
    private final String secretKey

    public SimpleDBClientFactory(String accessKey, String secretKey)
    {
        this.accessKey = accessKey
        this.secretKey = secretKey
    }

    public AmazonSimpleDBClient simpleDBClient()
    {
        final client = new AmazonSimpleDBClient(new BasicAWSCredentials(accessKey, secretKey))
        client.setRegion(getRegion(Regions.EU_WEST_1))
        return client
    }
}
