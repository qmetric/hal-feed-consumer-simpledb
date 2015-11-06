package com.qmetric.feed.consumer.simpledb

import static java.lang.System.currentTimeMillis

class DomainNameFactory {
    public static userPrefixedDomainName(String username)
    {
        "${username}-${currentTimeMillis()}".toString()
    }
}
