package io.flatf.foundation.common.functional;

import javax.annotation.Nonnull;

/**
 * @param <T>
 * @author yellow013
 */
@FunctionalInterface
public interface Processor<T> {

    /**
     * @param t T
     */
    void process(@Nonnull T t) throws RuntimeException;

}
