/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Following class tracks futures and when needed tries to wait for them
 */
public class FutureTracker<T> {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Condition onRemove = lock.writeLock().newCondition();
    /** Guarded by {@link #lock} */
    private final Set<CompletableFuture<T>> futures;
    private volatile boolean enabled;
    private final int limit;
    private final boolean hasLimit;

    private FutureTracker(int limit) {
        this.futures = new HashSet<>();
        this.enabled = true;
        this.limit = limit;
        this.hasLimit = limit > 0;
    }

    /**
     * Creates unlimited FutureTracker
     * @param <T> is trackable {@link CompletableFuture} type
     * @return instance of FutureTracker class
     */
    public static <T> FutureTracker<T> create() {
        return new FutureTracker<>(0);
    }

    /**
     * Creates FutureTracker with limit size
     * @param <T> is trackable {@link CompletableFuture} type
     * @param limit is max number of trackable futures if positive value, otherwise tracker unlimited
     * @return instance of FutureTracker class
     */
    public static <T> FutureTracker<T> create(int limit) {
        return new FutureTracker<>(limit);
    }

    /**
     * Track a future
     * @param future to be tracked
     * @return true if future isn't done and is added for tracking otherwise false
     */
    public boolean track(CompletableFuture<T> future) {
        if (enabled) {
            if (future.isDone()) {
                return false;
            }
            lock.writeLock().lock();
            try {
                if (hasLimit && futures.size() == limit) {
                    return false;
                }
                futures.add(future);
            } finally {
                lock.writeLock().unlock();
            }
            future.whenComplete((res, ex) -> {
                lock.writeLock().lock();
                try {
                    futures.remove(future);
                    onRemove.signalAll();
                } finally {
                    lock.writeLock().unlock();
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Try to add the future for tracking during timeoutMillis
     * @param future to be tracked
     * @param timeoutMillis the maximum time in milliseconds to wait for free space for adding
     * @return true if future isn't done and is added for tracking during timeout otherwise false
     * @throws InterruptedException - if any thread has interrupted the current thread.
     */
    public boolean track(CompletableFuture<T> future, long timeoutMillis) throws InterruptedException {
        if (enabled) {
            if (future.isDone()) {
                return false;
            }
            long endNanos = System.nanoTime() + timeoutMillis * 1_000_000L;

            do {
                lock.writeLock().lock();
                try {
                    if (future.isDone()) {
                        return false;
                    }
                    if (!hasLimit || futures.size() < limit) {
                        futures.add(future);
                        break;
                    }

                    long remainingTime = endNanos - System.nanoTime();
                    if (remainingTime <= 0) {
                        return false;
                    }

                    if (!onRemove.await(remainingTime, TimeUnit.NANOSECONDS)) {
                        return false;
                    }
                    if (future.isDone()) {
                        return false;
                    }
                    if (futures.size() < limit) {
                        futures.add(future);
                        break;
                    }
                } finally {
                    lock.writeLock().unlock();
                }
            } while (System.nanoTime() < endNanos);

            future.whenComplete((res, ex) -> {
                lock.writeLock().lock();
                try {
                    futures.remove(future);
                    onRemove.signalAll();
                } finally {
                    lock.writeLock().unlock();
                }
            });
            return true;
        }
        return false;
    }

    /**
     * Stops accepting new futures and waits for
     * currently tracked futures
     */
    public void awaitRemaining () {
        awaitRemaining(-1);
    }

    /**
     * Disables traker and waits for tracked futures, cancels futures after timeout.
     * Cancels all futures without wait if passed 0.
     * Waits without timeout if passed negative.
     * @param timeoutMillis milliseconds to wait
     */
    public void awaitRemaining (long timeoutMillis) {
        long timeoutNanos = timeoutMillis * 1_000_000;
        long startNanos = System.nanoTime();
        this.enabled = false;

        List<CompletableFuture<T>> remainingFutures;
        lock.readLock().lock();
        try {
            if (futures.isEmpty()) {
                return;
            }
            remainingFutures = new ArrayList<>(futures);
        } finally {
            lock.readLock().unlock();
        }

        for (CompletableFuture<T> future : remainingFutures) {
            long curNanos = System.nanoTime();

            try {
                if (!future.isDone()) {
                    if (timeoutMillis < 0) {
                        future.get();
                    } else {
                        if (curNanos - startNanos >= timeoutNanos) {
                            future.cancel(true);
                        } else {
                            future.get(timeoutNanos - (curNanos - startNanos), TimeUnit.NANOSECONDS);
                        }
                    }
                }
            } catch (ExecutionException | TimeoutException e) {
                // do nothing
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Informational method for getting remaining number of futures
     * @return number of unfinished futures
     */
    public int remaining() {
        lock.readLock().lock();
        try {
            return futures.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Limit of trackable features if positive value, otherwise tracker unlimited
     * @return max number of trackable futures if positive value, otherwise tracker unlimited
     */
    public int limit() {
        return limit;
    }

    /**
     * Has tracker got limit for number of trackable futures
     * @return true if {@link #limit()} is positive, otherwise false
     */
    public boolean hasLimit() {
        return hasLimit;
    }

    /**
     * Informs if there are any active futures
     * @return true if there are no active futures
     */
    public boolean isEmpty () {
        return remaining() == 0;
    }
}
