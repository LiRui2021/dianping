package com.hmdp.utils;

public interface ILock {
    boolean tryLock(long timeoutsec);

    void unlock();
}
