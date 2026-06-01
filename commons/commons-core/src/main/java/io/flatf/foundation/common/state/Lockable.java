package io.flatf.foundation.common.state;

public interface Lockable {

    boolean isLocked();

    boolean tryLock();

    void unlock();

}
