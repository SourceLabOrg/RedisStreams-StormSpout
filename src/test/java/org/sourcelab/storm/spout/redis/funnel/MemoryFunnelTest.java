package org.sourcelab.storm.spout.redis.funnel;

import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.Message;
import org.sourcelab.storm.spout.redis.RedisStreamSpoutConfig;
import org.sourcelab.storm.spout.redis.failhandler.RetryFailedTuples;
import org.sourcelab.storm.spout.redis.example.TestTupleConverter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemoryFunnelTest {

    /**
     * Smoke test passing messages through the funnel.
     */
    @Test
    void testPassingMessages() {
        // Create config
        final RedisStreamSpoutConfig config = RedisStreamSpoutConfig.newBuilder()
            .withHost("host")
            .withPort(123)
            .withGroupName("GroupName")
            .withStreamKey("Key")
            .withConsumerIdPrefix("ConsumerId")
            .withNoRetryFailureHandler()
            .withTupleConverter(new TestTupleConverter())
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
        final RedisStreamSpoutConfig config = RedisStreamSpoutConfig.newBuilder()
            .withHost("host")
            .withPort(123)
            .withGroupName("GroupName")
            .withStreamKey("Key")
            .withConsumerIdPrefix("ConsumerId")
            .withNoRetryFailureHandler()
            .withTupleConverter(new TestTupleConverter())
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

    /**
     * Smoke test failure handler.
     */
    @Test
    void test_failureHandler() {
        // Create config
        final RedisStreamSpoutConfig config = RedisStreamSpoutConfig.newBuilder()
            .withHost("host")
            .withPort(123)
            .withGroupName("GroupName")
            .withStreamKey("Key")
            .withConsumerIdPrefix("ConsumerId")
            .withFailureHandler(new RetryFailedTuples(2))
            .withTupleConverter(new TestTupleConverter())
            .build();

        final Map<String, Object> stormConfig = new HashMap<>();

        // Create some messages
        // Create some messages
        final String msgId1 = "MyMsgId1";
        final Message message1 = new Message(msgId1, Collections.singletonMap("Key1", "Value1"));

        final String msgId2 = "MyMsgId2";
        final Message message2 = new Message(msgId2, Collections.singletonMap("Key2", "Value2"));

        final String msgId3 = "MyMsgId3";
        final Message message3 = new Message(msgId3, Collections.singletonMap("Key3", "Value3"));

        // Create funnel
        final MemoryFunnel funnel = new MemoryFunnel(config, stormConfig);

        // Push msgs into funnel
        funnel.addMessage(message1);
        funnel.addMessage(message2);
        funnel.addMessage(message3);

        // Take first msg out, should be first msg
        Message resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId1, resultMessage.getId());

        // Lets fail this message
        assertTrue(funnel.failMessage(resultMessage.getId()));

        // When we ask for another message it should give us the failed one back again
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId1, resultMessage.getId());

        // Lets ask for the next message, should be msg 2
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId2, resultMessage.getId());

        // Lets fail msg1 and msg 2
        assertTrue(funnel.failMessage(msgId1));
        assertTrue(funnel.failMessage(msgId2));

        // The next two messages should be msg 1 and 2 again
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId1, resultMessage.getId());
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId2, resultMessage.getId());

        // If we fail msg1 and msg2 again
        assertFalse(funnel.failMessage(msgId1), "msg1 should be rejected");
        assertTrue(funnel.failMessage(msgId2), "msg2 should be accepted");

        // Asking for the next two messages should give us msg 2 and 3
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId2, resultMessage.getId());
        resultMessage = funnel.nextMessage();
        assertNotNull(resultMessage, "Should have message");
        assertEquals(msgId3, resultMessage.getId());

        // Next msg should be null
        assertNull(funnel.nextMessage());

        // If we ack msg2 and 3
        funnel.ackMessage(msgId2);
        funnel.ackMessage(msgId3);

        // And then ask for the next 3 acks, it should give us msgId1, 2, and 3.
        assertEquals(msgId1, funnel.nextAck());
        assertEquals(msgId2, funnel.nextAck());
        assertEquals(msgId3, funnel.nextAck());

        // And no more
        assertNull(funnel.nextAck());
        assertNull(funnel.nextAck());
    }

}