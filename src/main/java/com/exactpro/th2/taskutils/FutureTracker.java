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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(FutureTracker.class);
    private final Duration DURATION_MINUTE = Duration.ofMinutes(1);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Condition onRemove = lock.writeLock().newCondition();
    /**
     * Guarded by {@link #lock}
     */
    private final Set<CompletableFuture<T>> futures;
    /**
     * Changed under write lock of {@link #lock}
     */
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
     * @deprecated Use {@link FutureTracker#createUnlimited()} instead
     */
    @Deprecated
    public FutureTracker() {
        this(0);
    }

    /**
     * Creates unlimited FutureTracker
     *
     * @param <T> is trackable {@link CompletableFuture} type
     * @return instance of FutureTracker class
     */
    public static <T> FutureTracker<T> createUnlimited() {
        return new FutureTracker<>(0);
    }

    /**
     * Creates FutureTracker with limit size
     *
     * @param <T>   is trackable {@link CompletableFuture} type
     * @param limit is max number of trackable futures if positive value, otherwise tracker unlimited
     * @return instance of FutureTracker class
     */
    public static <T> FutureTracker<T> create(int limit) {
        return new FutureTracker<>(limit);
    }

    /**
     * Try to add the future for tracking
     *
     * @param future to be tracked
     * @return true if future is already done or is added for tracking otherwise false
     * @throws InterruptedException  if any thread has interrupted the current thread.
     * @throws IllegalStateException if future tracker is disabled
     */
    public boolean track(CompletableFuture<T> future) throws InterruptedException {
        while (!track(future, DURATION_MINUTE)) {
            LOGGER.warn("Future can't be added to track list for longer than {}", DURATION_MINUTE);
        }
        return true;
    }

    /**
     * Try to add the future for tracking during timeout
     *
     * @param future  to be tracked
     * @param timeout the maximum timeout to wait for free space for adding
     * @return true if future is already done or is added for tracking during timeout otherwise false
     * @throws InterruptedException  if any thread has interrupted the current thread.
     * @throws IllegalStateException if future tracker is disabled
     */
    public boolean track(CompletableFuture<T> future, Duration timeout) throws InterruptedException {
        if (enabled) {
            if (future.isDone()) {
                return true;
            }
            long endNanos = System.nanoTime() + timeout.toNanos();

            lock.writeLock().lock();
            try {
                if (future.isDone()) {
                    return true;
                }
                if (!hasLimit || futures.size() < limit) {
                    addFuture(future);
                    return true;
                }

                long remainingTime = endNanos - System.nanoTime();
                if (remainingTime <= 0) {
                    return future.isDone();
                }

                if (!onRemove.await(remainingTime, TimeUnit.NANOSECONDS)) {
                    return future.isDone();
                }
                if (future.isDone()) {
                    return true;
                }
                if (futures.size() < limit) {
                    addFuture(future);
                    return true;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        throw new IllegalStateException("Future tracker is already disabled");
    }

    /**
     * Stops accepting new futures and waits for
     * currently tracked futures
     */
    public void awaitRemaining() throws InterruptedException {
        awaitRemaining(Duration.ofMillis(-1));
    }

    /**
     * Disables traker and waits for tracked futures, cancels futures after timeout.
     * Cancels all futures without wait if passed 0.
     * Waits without timeout if passed negative.
     *
     * @param timeoutMillis milliseconds to wait
     * @deprecated use {@link #awaitRemaining(Duration)} instead
     */
    @Deprecated
    public void awaitRemaining(long timeoutMillis) throws InterruptedException {
        awaitRemaining(Duration.ofMillis(timeoutMillis));
    }

    /**
     * Disables traker and waits for tracked futures, cancels futures after timeout.
     * Cancels all futures without wait if passed 0.
     * Waits without timeout if passed negative.
     *
     * @param timeout duration to wait
     */
    public void awaitRemaining(Duration timeout) throws InterruptedException {
        long timeoutNanos = timeout.toNanos();
        long startNanos = System.nanoTime();

        List<CompletableFuture<T>> remainingFutures;
        lock.writeLock().lock();
        try {
            this.enabled = false;
            if (futures.isEmpty()) {
                return;
            }
            remainingFutures = new ArrayList<>(futures);
        } finally {
            lock.writeLock().unlock();
        }

        for (CompletableFuture<T> future : remainingFutures) {
            long curNanos = System.nanoTime();

            try {
                if (!future.isDone()) {
                    if (timeoutNanos < 0) {
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
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }

    /**
     * Informational method for getting remaining number of futures
     *
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
     *
     * @return max number of trackable futures if positive value, otherwise tracker unlimited
     */
    public int limit() {
        return limit;
    }

    /**
     * Has tracker got limit for number of trackable futures
     *
     * @return true if {@link #limit()} is positive, otherwise false
     */
    public boolean hasLimit() {
        return hasLimit;
    }

    /**
     * Informs if there are any active futures
     *
     * @return true if there are no active futures
     */
    public boolean isEmpty() {
        return remaining() == 0;
    }

    /**
     * Informs if future tracker can accept new futures or not
     *
     * @return current state of {@link FutureTracker} instance
     */
    public boolean isEnabled() {
        return enabled;
    }

    private void addFuture(CompletableFuture<T> future) {
        if (!enabled) {
            throw new IllegalStateException("Future tracker is disabled before tracking start");
        }
        futures.add(future);
        future.whenComplete((res, ex) -> {
            lock.writeLock().lock();
            try {
                futures.remove(future);
                onRemove.signalAll();
            } finally {
                lock.writeLock().unlock();
            }
        });
    }
}
