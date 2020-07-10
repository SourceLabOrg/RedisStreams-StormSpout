package org.sourcelab.storm.spout.redis.funnel;

import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.Configuration;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;
import org.sourcelab.storm.spout.redis.util.TestTupleConverter;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class MemoryFunnelTest {

    /**
     * Smoke test passing messages through the funnel.
     */
    @Test
    void testPassingMessages() {
        // Create config
        final Configuration config = Configuration.newBuilder()
            .withHost("host")
            .withPort(123)
            .withGroupName("GroupName")
            .withStreamKey("Key")
            .withConsumerId("ConsumerId")
            .withFailureHandlerClass(NoRetryHandler.class)
            .withTupleConverterClass(TestTupleConverter.class)
            .build();

        // Create some messages
        final String msgId1 = "MyMsgId1";
        final Message message1 = new Message(msgId1, Collections.singletonMap("Key1", "Value1"));

        final String msgId2 = "MyMsgId2";
        final Message message2 = new Message(msgId2, Collections.singletonMap("Key2", "Value2"));

        final String msgId3 = "MyMsgId3";
        final Message message3 = new Message(msgId3, Collections.singletonMap("Key3", "Value3"));

        // Create funnel
        final MemoryFunnel funnel = new MemoryFunnel(config, new HashMap<>());

        // Ask for message, should be empty
        assertNull(funnel.nextMessage(), "Should have no messages");
        assertNull(funnel.nextMessage(), "Should have no messages");
        assertNull(funnel.nextMessage(), "Should have no messages");

        // Push messages into funnel
        funnel.addMessage(message1);
        funnel.addMessage(message2);
        funnel.addMessage(message3);

        // Ask for messages
        Message resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId1, resultMessage.getId());

        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId2, resultMessage.getId());

        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId3, resultMessage.getId());

        // Ask for message, should be empty
        assertNull(funnel.nextMessage(), "Should have no messages");
        assertNull(funnel.nextMessage(), "Should have no messages");
        assertNull(funnel.nextMessage(), "Should have no messages");
    }

    /**
     * Smoke test passing acks through the funnel.
     */
    @Test
    void testPassingAcks() {
        // Create config
        final Configuration config = Configuration.newBuilder()
            .withHost("host")
            .withPort(123)
            .withGroupName("GroupName")
            .withStreamKey("Key")
            .withConsumerId("ConsumerId")
            .withFailureHandlerClass(NoRetryHandler.class)
            .withTupleConverterClass(TestTupleConverter.class)
            .build();

        // Create some messages
        final String msgId1 = "MyMsgId1";
        final String msgId2 = "MyMsgId2";
        final String msgId3 = "MyMsgId3";

        // Create funnel
        final MemoryFunnel funnel = new MemoryFunnel(config, new HashMap<>());

        // Ask for acks, should be empty
        assertNull(funnel.nextAck(), "Should have no acks");
        assertNull(funnel.nextAck(), "Should have no acks");
        assertNull(funnel.nextAck(), "Should have no acks");

        // Push acks into funnel
        funnel.ackMessage(msgId1);
        funnel.ackMessage(msgId2);
        funnel.ackMessage(msgId3);

        // Ask for acks
        String resultMsgId = funnel.nextAck();
        assertNotNull(resultMsgId, "Should have ack");
        assertEquals(msgId1, resultMsgId);

        resultMsgId = funnel.nextAck();
        assertNotNull(resultMsgId, "Should have ack");
        assertEquals(msgId2, resultMsgId);

        resultMsgId = funnel.nextAck();
        assertNotNull(resultMsgId, "Should have ack");
        assertEquals(msgId3, resultMsgId);

        // Ask for acks, should be empty
        assertNull(funnel.nextAck(), "Should have no acks");
        assertNull(funnel.nextAck(), "Should have no acks");
        assertNull(funnel.nextAck(), "Should have no acks");
    }

}