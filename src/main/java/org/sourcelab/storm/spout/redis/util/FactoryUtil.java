package org.sourcelab.storm.spout.redis.util;

import java.util.Objects;

/**
 * Utility class for creating new instances.
 */
public class FactoryUtil {

    /**
     * Utility method for instantiating new instance from a package/class name.
     * @param classStr Fully qualified classname.
     * @param <T> Instance you are creating.
     * @return Newly created instance.
     */
    public static synchronized <T> T createNewInstance(final String classStr) {
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
