package org.sourcelab.storm.spout.redis.util;

import org.sourcelab.storm.spout.redis.TupleConverter;
import org.sourcelab.storm.spout.redis.FailureHandler;

import java.util.Objects;

/**
 * Utility class for creating new instances.
 */
public class FactoryUtil {

    /**
     * Create a new FailureHandler instance being passed a class name.
     * @param classStr Class name of FailureHandler instance to create.
     * @return FailureHandler instance.
     */
    public static FailureHandler newFailureHandler(final String classStr) {
        try {
            return createNewInstance(classStr);
        } catch (final ClassCastException exception) {
            throw new IllegalArgumentException(
                "Passed value of '" + classStr + "' must implement interface " + FailureHandler.class.getName()
                    + ": " + exception.getMessage(), exception
            );
        }
    }

    /**
     * Create a new TupleConverter instance being passed a class name.
     * @param classStr Class name of TupleConverter instance to create.
     * @return TupleConverter instance.
     */
    public static TupleConverter newTupleConverter(final String classStr) {
        try {
            return createNewInstance(classStr);
        } catch (final ClassCastException exception) {
            throw new IllegalArgumentException(
                "Passed value of '" + classStr + "' must implement interface " + TupleConverter.class.getName()
                + ": " + exception.getMessage(), exception
            );
        }
    }

    /**
     * Utility method for instantiating new instance from a package/class name.
     * @param classStr Fully qualified classname.
     * @param <T> Instance you are creating.
     * @return Newly created instance.
     */
    private static <T> T createNewInstance(final String classStr) {
        Objects.requireNonNull(classStr);

        try {
            @SuppressWarnings("unchecked")
            final Class<? extends T> clazz = (Class<? extends T>) Class.forName(classStr);
            return clazz.newInstance();
        } catch (final ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }
}
