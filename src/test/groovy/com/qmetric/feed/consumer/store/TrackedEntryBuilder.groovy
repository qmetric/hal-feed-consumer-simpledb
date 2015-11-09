package com.qmetric.feed.consumer.store

import com.qmetric.feed.consumer.EntryId
import com.qmetric.feed.consumer.TrackedEntry
import org.joda.time.DateTime

class TrackedEntryBuilder {

    private EntryId entryId = EntryId.of("123")
    private DateTime created = new DateTime(2011, 1, 10, 12, 0, 0, 0)
    private DateTime seenAt = new DateTime(2011, 1, 10, 12, 0, 0, 0)
    private int retries = 0

    static TrackedEntryBuilder trackedEntryBuilder() {
        new TrackedEntryBuilder()
    }

    TrackedEntryBuilder withEntryId(EntryId entryId) {
        this.entryId = entryId

        this
    }

    TrackedEntryBuilder withCreated(DateTime created) {
        this.created = created

        this
    }

    TrackedEntryBuilder withSeenAt(DateTime seenAt) {
        this.seenAt = seenAt

        this
    }

    TrackedEntryBuilder withNonEmptySeenAt() {
        withSeenAt(DateTime.now())
    }

    TrackedEntryBuilder withNoRetries() {
        withRetries(0)
    }

    TrackedEntryBuilder withRetries(int retries) {
        this.retries = retries

        this
    }

    TrackedEntry build() {
        new TrackedEntry(entryId, created, seenAt, retries)
    }
}
