/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.taskutils;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StartableRunnable implements Runnable {
    private volatile boolean ready;
    private volatile boolean started;

    private final Object waitSignal;
    private final Object startSignal;
    private final Runnable runnable;
    private final Lock readinessLock;
    private final Lock startingLock;

    private StartableRunnable(Runnable runnable) {
        this.runnable = runnable;

        this.waitSignal = new Object();
        this.startSignal = new Object();
        this.readinessLock = new ReentrantLock();
        this.startingLock = new ReentrantLock();
    }


    public static StartableRunnable of(Runnable runnable) {
        return new StartableRunnable(runnable);
    }


    public void awaitReadiness() {
        readinessLock.lock();
        if (!isReady()) {
            _unlockAndWait(readinessLock, waitSignal);
        } else
            readinessLock.unlock();
    }


    private void notifyReadinessAwaiters() {
        _notifyAll(waitSignal);
    }


    private void awaitStart() {
        startingLock.lock();
        if (!isStarted()) {
            _unlockAndWait(startingLock, startSignal);
        } else
            startingLock.unlock();
    }


    public void start() {
        readinessLock.lock();
        if (!isReady()) {
            readinessLock.unlock();
            throw new IllegalStateException("Not ready yet");
        }
        readinessLock.unlock();

        setStarted();
        _notifyAll(startSignal);
    }


    public boolean isReady() {
        readinessLock.lock();
        boolean val = ready;
        readinessLock.unlock();

        return val;
    }


    private void setReady() {
        readinessLock.lock();
        ready = true;
        readinessLock.unlock();
    }


    public boolean isStarted() {
        startingLock.lock();
        boolean val = started;
        startingLock.unlock();

        return val;
    }


    private void setStarted() {
        startingLock.lock();
        if (started)
            throw new RuntimeException("Already started");
        started = true;
        startingLock.unlock();
    }




    @Override
    public void run() {

        setReady();
        notifyReadinessAwaiters();
        awaitStart();

        runnable.run();
    }


    private void _unlockAndWait(Lock lock, Object o) {
        synchronized (o) {
            try {
                if (lock != null)
                    lock.unlock();
                o.wait();
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }
        }
    }

    private void _notifyAll(Object o) {
        synchronized (o) {
            o.notifyAll();
        }
    }
}
