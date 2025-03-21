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

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class FutureTrackerTest {
    private final long NO_DELAY_MILLIS = 75;
    private final long DELAY_MILLIS = 100;

    @FunctionalInterface
    private interface Task {
        void invoke() throws InterruptedException;
    }

    private static class SleepingRunnable implements Runnable {
        final long sleepTimeMillis;
        public SleepingRunnable(long sleepTimeMillis) {
            this.sleepTimeMillis = sleepTimeMillis;
        }
        @Override
        public void run() {
            try {
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }

    private CompletableFuture<Integer> getFutureWithException () {
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });
    }

    private CompletableFuture<Integer> getFutureWithDelay (StartableRunnable waitingRunnable) {
        return CompletableFuture.supplyAsync(() -> {
            waitingRunnable.run();
            return 0;
        });
    }

    private CompletableFuture<Integer> chainFuture (CompletableFuture<Integer> future) {
        return future.thenApply((res) -> {
            try {
                Thread.sleep(DELAY_MILLIS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return res;
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void assertLessDuration(long timeoutMillis, Task task) throws InterruptedException {
        long start = System.nanoTime();
        task.invoke();
        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        assertThat(actualTrackingMillis).isLessThan(timeoutMillis);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertGreaterDuration(long timeoutMillis, Task task) throws InterruptedException {
        long start = System.nanoTime();
        task.invoke();
        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(timeoutMillis);
    }

    @Test
    public void testEmptyTracker () throws InterruptedException {
        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isTrue());
    }

    @Test
    public void testTrackingSingleFuture () throws InterruptedException {
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
        futureTracker.track(getFutureWithDelay(waitingRunnable));

        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isFalse());
        assertGreaterDuration(DELAY_MILLIS, () -> {
            waitingRunnable.awaitReadiness();
            waitingRunnable.start();
            futureTracker.awaitRemaining();
        });
    }

    @Test
    public void testTrackingFutureWithException () throws InterruptedException {
        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.track(getFutureWithException());

        assertLessDuration(NO_DELAY_MILLIS, futureTracker::awaitRemaining);
    }

    @Test
    public void testTracking5Futures() throws InterruptedException {
        int tasks = 5;
        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < tasks; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay(waitingRunnable);
            if (i != 0) {
                curFuture = chainFuture(lastFuture);
            }
            futureTracker.track(curFuture);
            lastFuture = curFuture;
        }
        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isFalse());
        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.remaining()).isEqualTo(5));
        assertGreaterDuration(tasks * DELAY_MILLIS, () -> {
            waitingRunnable.awaitReadiness();
            waitingRunnable.start();
            futureTracker.awaitRemaining();
        });
    }

    @Test
    public void testAwaitZeroTimeout() throws InterruptedException {
        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.track(getFutureWithDelay(waitingRunnable));

        assertLessDuration(NO_DELAY_MILLIS, () -> {
            waitingRunnable.awaitReadiness();
            waitingRunnable.start();
            futureTracker.awaitRemaining(0);
        });
    }

    @Test
    public void testAwaitUntilSizeTracker() throws InterruptedException {
        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
        futureTracker.track(getFutureWithDelay(waitingRunnable));

        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.awaitUntilSizeNotMore(2, 0)).isTrue());
        assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.awaitUntilSizeNotMore(1, 0)).isTrue());
        assertGreaterDuration(DELAY_MILLIS, () -> assertThat(futureTracker.awaitUntilSizeNotMore(0, DELAY_MILLIS)).isFalse());

        assertGreaterDuration(DELAY_MILLIS, () -> {
            waitingRunnable.awaitReadiness();
            waitingRunnable.start();
            assertThat(futureTracker.awaitUntilSizeNotMore(0, DELAY_MILLIS * 2)).isTrue();
        });
    }
}
