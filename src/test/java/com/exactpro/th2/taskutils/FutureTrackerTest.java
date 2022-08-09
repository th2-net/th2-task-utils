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

import java.util.concurrent.CompletableFuture;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;


public class FutureTrackerTest {
    private final long NO_DELAY_MILLIS = 75;
    private final long DELAY_MILLIS = 100;

    private class SleepingRunnable implements Runnable {
        final long sleepTimeMillis;
        public SleepingRunnable(long sleepTimeMillis) {
            this.sleepTimeMillis = sleepTimeMillis;
        }
        @Override
        public void run() {
            try {
                Thread.sleep(sleepTimeMillis);
            } catch (InterruptedException e) {

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

    @Test
    public void testEmptyTracker () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        Assertions.assertThat(futureTracker.isEmpty()).isTrue();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }

    @Test
    public void testTrackingSingleFuture () {
        long expectedTrackingMillis = DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

        futureTracker.track(getFutureWithDelay(waitingRunnable));
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();

        waitingRunnable.awaitReadiness();
        waitingRunnable.start();

        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingMillis);
    }

    @Test
    public void testTrackingFutureWithException () {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        long start = System.nanoTime();
        futureTracker.track(getFutureWithException());
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }

    @Test
    public void testTracking5Futures() {
        long expectedTrackingTime = 5 * DELAY_MILLIS;

        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

        FutureTracker<Integer> futureTracker = new FutureTracker<>();

        CompletableFuture<Integer> lastFuture = null;
        for (int i = 0; i < 5; i ++) {
            CompletableFuture<Integer> curFuture = getFutureWithDelay(waitingRunnable);
            if (i != 0) {
                curFuture = chainFuture(lastFuture);
            }
            futureTracker.track(curFuture);
            lastFuture = curFuture;
        }
        Assertions.assertThat(futureTracker.isEmpty()).isFalse();
        Assertions.assertThat(futureTracker.remaining()).isEqualTo(5);

        waitingRunnable.awaitReadiness();
        waitingRunnable.start();

        long start = System.nanoTime();
        futureTracker.awaitRemaining();

        long actualTrackingMillis = (System.nanoTime() - start)/1_000_000;
        Assertions.assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(expectedTrackingTime);
    }

    @Test
    public void testAwaitZeroTimeout() {
        long expectedTrackingMillis = NO_DELAY_MILLIS;

        StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

        FutureTracker<Integer> futureTracker = new FutureTracker<>();
        futureTracker.track(getFutureWithDelay(waitingRunnable));

        waitingRunnable.awaitReadiness();
        waitingRunnable.start();

        long start = System.nanoTime();
        futureTracker.awaitRemaining(0);


        long actualTrackingMillis = (System.nanoTime() - start) / 1_000_000;
        Assertions.assertThat(actualTrackingMillis).isLessThan(expectedTrackingMillis);
    }
}
