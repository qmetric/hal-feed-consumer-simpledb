package com.qmetric.feed.consumer.simpledb.utils

import static com.google.common.base.Preconditions.checkState
import static java.lang.System.getenv
import static org.apache.commons.lang3.StringUtils.isBlank

class TestEnvironment
{
    private static final String ACCESS_KEY_PROPERTY = 'HAL_CONSUMER_IT_AWS_ACCESS_KEY'

    private static final String SECRET_KEY_PROPERTY = 'HAL_CONSUMER_IT_AWS_SECRET_KEY'

    static void verifyEnvironment()
    {
        checkState(!isBlank(accessKey()), 'Missing env variable %s', ACCESS_KEY_PROPERTY)
        checkState(!isBlank(secretKey()), 'Missing env variable %s', SECRET_KEY_PROPERTY)
    }

    static accessKey()
    {
        getenv(ACCESS_KEY_PROPERTY)
    }

    static secretKey()
    {
        getenv(SECRET_KEY_PROPERTY)
    }
}
