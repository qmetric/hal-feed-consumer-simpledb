package com.qmetric.feed.consumer.simpledb.utils

import spark.Request
import spark.Response
import spark.Route

import static groovy.json.JsonOutput.toJson

public class MockEntryHandler extends Route
{
    public MockEntryHandler()
    {
        super("/feed/:id")
    }

    @Override Object handle(final Request request, final Response response)
    {
        def eId = request.params('id')
        return toJson( [//
                _id: "${eId}",
                _published: "24/05/2013 00:0${eId}:00",
                _links: [self: [href: "http://localhost:15000/feed/${eId}"]],
                type: (eId == 1 ? "error" : "ok") //
        ])
    }
}
