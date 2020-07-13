package org.sourcelab.storm.spout.redis;

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
        assertNotNull(message.getBody());
        assertEquals(3, message.getBody().size());
        assertTrue(message.getBody().containsKey("Key1"));
        assertEquals("Value1", message.getBody().get("Key1"));
        assertTrue(message.getBody().containsKey("Key2"));
        assertEquals("Value2", message.getBody().get("Key2"));
        assertTrue(message.getBody().containsKey("Key3"));
        assertEquals("Value3", message.getBody().get("Key3"));

        // Validate message is not modifiable/immutable.
        assertThrows(UnsupportedOperationException.class, () -> message.getBody().put("NewValue", "AnotherValue"));
    }
}