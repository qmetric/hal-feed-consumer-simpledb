package com.qmetric.feed.consumer.simpledb.utils

import spark.Request
import spark.Response
import spark.Route

import static groovy.json.JsonOutput.toJson
import static org.apache.commons.lang3.StringUtils.isBlank

public class MockFeedHandler extends Route
{
    private final int feedSize
    private final int pageSize

    public MockFeedHandler(String path, int feedSize, int pageSize)
    {
        super(path)
        this.pageSize = pageSize
        this.feedSize = feedSize
    }

    @Override Object handle(final Request request, final Response response)
    {

        def pageIndex = request.queryParams("upToEntryId");
        if (isBlank(pageIndex))
        {
            pageIndex = lastEntryIndex
        }
        else
        {
            pageIndex = Integer.valueOf(pageIndex)
        }
        def entries = generateEntries(pageIndex)
        response.status(200)
        return entries
    }

    private int getLastEntryIndex()
    {
        feedSize
    }

    private generateEntries(int upToEntryId)
    {
        def fromEntryId = pageStart(upToEntryId)
        def entries = (fromEntryId..upToEntryId).reverse().collect { eId ->
            [//
                    _id: "${eId}",
                    _published: "24/05/2013 00:0${eId}:00",
                    _links: [self: [href: "http://localhost:15000/feed/${eId}"]],
                    type: (eId == 1 ? "error" : "ok") //
            ]
        }

        def page = [_links: [self: [href: "http://localhost:15000/feed?upToEntryId=${upToEntryId}"]], _embedded: [entries: entries]]

        if (fromEntryId > 1)
        {
            page._links.next = [href: "http://localhost:15000/feed?upToEntryId=${fromEntryId - 1}"]
        }

        def previousLinkFromEntryId = upToEntryId + pageSize

        if (previousLinkFromEntryId <= feedSize)
        {
            page._links.previous = [href: "http://localhost:15000/feed?upToEntryId=${previousLinkFromEntryId}"]
        }

        return toJson(page)
    }

    private int pageStart(int lastEntryIndex)
    {
        lastEntryIndex > pageSize ? lastEntryIndex - pageSize + 1 : 1
    }
}