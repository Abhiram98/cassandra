package org.apache.cassandra.net;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MessagingServiceTest
{
    private final MessagingService messagingService = MessagingService.test();

    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
        {
            assert MessagingService.DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
            MessagingService.DroppedMessages droppedMessages = messagingService.droppedMessagesMap.get(verb);
            droppedMessages.metrics.dropped.mark();
            if (i % 2 == 0)
                droppedMessages.droppedCrossNodeTimeout.incrementAndGet();
            else
                droppedMessages.droppedInternalTimeout.incrementAndGet();
        }

        List<String> ret2 = new ArrayList<>();
        for (Map.Entry<MessagingService.Verb, MessagingService.DroppedMessages> entry2 : messagingService.droppedMessagesMap.entrySet())
        {
            MessagingService.Verb verb3 = entry2.getKey();
            MessagingService.DroppedMessages droppedMessages2 = entry2.getValue();

            int droppedInternalTimeout2 = droppedMessages2.droppedInternalTimeout.getAndSet(0);
            int droppedCrossNodeTimeout2 = droppedMessages2.droppedCrossNodeTimeout.getAndSet(0);
            if (droppedInternalTimeout2 > 0 || droppedCrossNodeTimeout2 > 0)
            {
                ret2.add(String.format("%s messages were dropped in last %d ms: %d for internal timeout and %d for cross node timeout",
                                       verb3,
                                       MessagingService.LOG_DROPPED_INTERVAL_IN_MS,
                                       droppedInternalTimeout2,
                                       droppedCrossNodeTimeout2));
            }
        }
        List<String> logs = ret2;
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));
        assertEquals(5000, (int) messagingService.getRecentlyDroppedMessages().get(verb.toString()));

        List<String> ret1 = new ArrayList<>();
        for (Map.Entry<MessagingService.Verb, MessagingService.DroppedMessages> entry1 : messagingService.droppedMessagesMap.entrySet())
        {
            MessagingService.Verb verb2 = entry1.getKey();
            MessagingService.DroppedMessages droppedMessages1 = entry1.getValue();

            int droppedInternalTimeout1 = droppedMessages1.droppedInternalTimeout.getAndSet(0);
            int droppedCrossNodeTimeout1 = droppedMessages1.droppedCrossNodeTimeout.getAndSet(0);
            if (droppedInternalTimeout1 > 0 || droppedCrossNodeTimeout1 > 0)
            {
                ret1.add(String.format("%s messages were dropped in last %d ms: %d for internal timeout and %d for cross node timeout",
                                       verb2,
                                       MessagingService.LOG_DROPPED_INTERVAL_IN_MS,
                                       droppedInternalTimeout1,
                                       droppedCrossNodeTimeout1));
            }
        }
        logs = ret1;
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
        {
            assert MessagingService.DROPPABLE_VERBS.contains(verb) : "Verb " + verb + " should not legally be dropped";
            MessagingService.DroppedMessages droppedMessages = messagingService.droppedMessagesMap.get(verb);
            droppedMessages.metrics.dropped.mark();
            if (i % 2 == 0)
                droppedMessages.droppedCrossNodeTimeout.incrementAndGet();
            else
                droppedMessages.droppedInternalTimeout.incrementAndGet();
        }

        List<String> ret = new ArrayList<>();
        for (Map.Entry<MessagingService.Verb, MessagingService.DroppedMessages> entry : messagingService.droppedMessagesMap.entrySet())
        {
            MessagingService.Verb verb1 = entry.getKey();
            MessagingService.DroppedMessages droppedMessages = entry.getValue();

            int droppedInternalTimeout = droppedMessages.droppedInternalTimeout.getAndSet(0);
            int droppedCrossNodeTimeout = droppedMessages.droppedCrossNodeTimeout.getAndSet(0);
            if (droppedInternalTimeout > 0 || droppedCrossNodeTimeout > 0)
            {
                ret.add(String.format("%s messages were dropped in last %d ms: %d for internal timeout and %d for cross node timeout",
                                      verb1,
                                      MessagingService.LOG_DROPPED_INTERVAL_IN_MS,
                                      droppedInternalTimeout,
                                      droppedCrossNodeTimeout));
            }
        }
        logs = ret;
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
        assertEquals(2500, (int) messagingService.getRecentlyDroppedMessages().get(verb.toString()));
    }

}
