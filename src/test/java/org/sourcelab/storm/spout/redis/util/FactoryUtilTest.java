package org.sourcelab.storm.spout.redis.util;

import org.junit.jupiter.api.Test;
import org.sourcelab.storm.spout.redis.FailureHandler;
import org.sourcelab.storm.spout.redis.TupleConverter;
import org.sourcelab.storm.spout.redis.failhandler.NoRetryHandler;
import org.sourcelab.storm.spout.redis.util.test.TestTupleConverter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FactoryUtilTest {

    /**
     * Smoke test over creating FailureHandler instances.
     */
    @Test
    void smokeTest_failureHandler() {
        final FailureHandler instance = FactoryUtil.newFailureHandler(NoRetryHandler.class.getName());
        assertNotNull(instance);
        assertEquals(NoRetryHandler.class, instance.getClass());
    }

    /**
     * Smoke test over creating TupleConverter instances.
     */
    @Test
    void smokeTest_tupleConverter() {
        final TupleConverter instance = FactoryUtil.newTupleConverter(TestTupleConverter.class.getName());
        assertNotNull(instance);
        assertEquals(TestTupleConverter.class, instance.getClass());
    }

    @Test
    void smokeTest_invalidClass_tupleConverter() {
        assertThrows(IllegalArgumentException.class, () -> FactoryUtil.newTupleConverter(String.class.getName()));
    }

    @Test
    void smokeTest_invalidClass_failureHandler() {
        assertThrows(IllegalArgumentException.class, () -> FactoryUtil.newFailureHandler(String.class.getName()));
    }
}