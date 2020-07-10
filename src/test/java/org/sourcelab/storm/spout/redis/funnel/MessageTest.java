package org.sourcelab.storm.spout.redis.funnel;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageTest {

    /**
     * Simple smoke test over DTO object.
     */
    @Test
    void smokeTest() {
        final String id = "MyId";
        final Map<String, String> body = new HashMap<>();
        body.put("Key1", "Value1");
        body.put("Key2", "Value2");
        body.put("Key3", "Value3");

        final Message message = new Message(id, body);

        // Validate Id
        assertEquals(id, message.getId());

        // Validate message
        assertNotNull(message.getMessage());
        assertEquals(3, message.getMessage().size());
        assertTrue(message.getMessage().containsKey("Key1"));
        assertEquals("Value1", message.getMessage().get("Key1"));
        assertTrue(message.getMessage().containsKey("Key2"));
        assertEquals("Value2", message.getMessage().get("Key2"));
        assertTrue(message.getMessage().containsKey("Key3"));
        assertEquals("Value3", message.getMessage().get("Key3"));

        // Validate message is not modifiable/immutable.
        assertThrows(UnsupportedOperationException.class, () -> message.getMessage().put("NewValue", "AnotherValue"));
    }
}