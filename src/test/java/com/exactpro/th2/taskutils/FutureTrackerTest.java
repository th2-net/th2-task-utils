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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 1, unit = SECONDS)
public class FutureTrackerTest {
    private final int LIMIT = 3;
    private final long NO_DELAY_MILLIS = 75;
    private final long DELAY_MILLIS = 100;
    private final Duration DELAY_DURATION = Duration.ofMillis(DELAY_MILLIS);

    private final List<StartableRunnable> runnableList = new ArrayList<>();

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

    private CompletableFuture<Integer> getFutureWithException() {
        return CompletableFuture.supplyAsync(() -> {
            throw new RuntimeException();
        });
    }

    private CompletableFuture<Integer> getFutureWithDelay(StartableRunnable waitingRunnable) {
        runnableList.add(waitingRunnable);
        return CompletableFuture.supplyAsync(() -> {
            waitingRunnable.run();
            return 0;
        });
    }

    private CompletableFuture<Integer> chainFuture(CompletableFuture<Integer> future) {
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
        long actualTrackingMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(actualTrackingMillis).isLessThan(timeoutMillis);
    }

    @SuppressWarnings("SameParameterValue")
    private void assertGreaterDuration(long timeoutMillis, Task task) throws InterruptedException {
        long start = System.nanoTime();
        task.invoke();
        long actualTrackingMillis = (System.nanoTime() - start) / 1_000_000;
        assertThat(actualTrackingMillis).isGreaterThanOrEqualTo(timeoutMillis);
    }

    @AfterEach
    public void afterEach() {
        for (StartableRunnable startableRunnable : runnableList) {
            if (!startableRunnable.isReady()) {
                startableRunnable.awaitReadiness();
            }
            if (!startableRunnable.isStarted()) {
                startableRunnable.start();
            }
        }
        runnableList.clear();
    }

    @Nested
    class Both {
        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testEmptyTracker(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isTrue());
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testTrackingSingleFuture(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            assertThat(futureTracker.track(getFutureWithDelay(waitingRunnable))).isTrue();

            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isFalse());
            assertGreaterDuration(DELAY_MILLIS, () -> {
                waitingRunnable.awaitReadiness();
                waitingRunnable.start();
                futureTracker.awaitRemaining();
            });
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testTrackingFutureWithException(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            assertThat(futureTracker.track(getFutureWithException())).isTrue();
            assertLessDuration(NO_DELAY_MILLIS, futureTracker::awaitRemaining);
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testTrackingLimitFutures(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

            CompletableFuture<Integer> lastFuture = null;
            for (int i = 0; i < LIMIT; i++) {
                CompletableFuture<Integer> curFuture = getFutureWithDelay(waitingRunnable);
                if (i != 0) {
                    curFuture = chainFuture(lastFuture);
                }
                assertThat(futureTracker.track(curFuture)).isTrue();
                lastFuture = curFuture;
            }
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.isEmpty()).isFalse());
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.remaining()).isEqualTo(LIMIT));
            assertGreaterDuration(LIMIT * DELAY_MILLIS, () -> {
                waitingRunnable.awaitReadiness();
                waitingRunnable.start();
                futureTracker.awaitRemaining();
            });
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testAwaitZeroTimeout(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));

            assertThat(futureTracker.track(getFutureWithDelay(waitingRunnable))).isTrue();

            assertLessDuration(NO_DELAY_MILLIS, () -> {
                waitingRunnable.awaitReadiness();
                waitingRunnable.start();
                futureTracker.awaitRemaining(Duration.ZERO);
            });
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testFutureWithDoneStatus(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);
            CompletableFuture<Integer> extraFuture = CompletableFuture.completedFuture(1);

            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture)).isTrue());
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture, DELAY_DURATION)).isTrue());

            assertThat(futureTracker.remaining()).isEqualTo(0);
        }

        @ParameterizedTest
        @ValueSource(ints = {0, LIMIT})
        public void testTrackFutureAfterStop(int limit) throws InterruptedException {
            FutureTracker<Integer> futureTracker = FutureTracker.create(limit);

            assertThat(futureTracker.isEnabled()).isTrue();
            assertLessDuration(NO_DELAY_MILLIS, futureTracker::awaitRemaining);
            assertThat(futureTracker.isEnabled()).isFalse();

            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            CompletableFuture<Integer> curFuture = getFutureWithDelay(waitingRunnable);

            assertLessDuration(NO_DELAY_MILLIS, () -> {
                IllegalStateException exception = assertThrows(IllegalStateException.class, () -> futureTracker.track(curFuture));
                assertThat(exception.getMessage()).isEqualTo("Future tracker is already disabled");
            });
            assertLessDuration(NO_DELAY_MILLIS, () -> {
                IllegalStateException exception = assertThrows(IllegalStateException.class, () -> futureTracker.track(curFuture, DELAY_DURATION));
                assertThat(exception.getMessage()).isEqualTo("Future tracker is already disabled");
            });
        }
    }

    @Nested
    class Unlimited {
        private final FutureTracker<Integer> futureTracker = FutureTracker.createUnlimited();

        @Test
        public void testLimit() {
            assertThat(futureTracker.limit()).isLessThanOrEqualTo(0);
            assertThat(futureTracker.hasLimit()).isFalse();
        }
    }

    @Nested
    class Limited {
        private final FutureTracker<Integer> futureTracker = FutureTracker.create(LIMIT);

        @Test
        public void testLimit() {
            assertThat(futureTracker.limit()).isGreaterThan(0);
            assertThat(futureTracker.hasLimit()).isTrue();
        }

        @Test
        public void testExtraFutureWithTimeout() throws InterruptedException {
            List<StartableRunnable> startables = new ArrayList<>();

            for (int i = 0; i < LIMIT; i++) {
                assertLessDuration(NO_DELAY_MILLIS, () -> {
                    StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
                    CompletableFuture<Integer> future = getFutureWithDelay(waitingRunnable);
                    assertThat(futureTracker.track(future)).isTrue();
                    startables.add(waitingRunnable);
                });
            }

            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            CompletableFuture<Integer> extraFuture = getFutureWithDelay(waitingRunnable);
            assertGreaterDuration(DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture, DELAY_DURATION)).isFalse());

            assertGreaterDuration(DELAY_MILLIS, () -> {
                startables.forEach(StartableRunnable::awaitReadiness);
                startables.forEach(StartableRunnable::start);

                assertThat(futureTracker.track(extraFuture, DELAY_DURATION.multipliedBy(2))).isTrue();
            });
        }

        @Test
        public void testExtraFutureWithNegativeTimeout() throws InterruptedException {
            Duration timeout = Duration.ofSeconds(-1);
            List<StartableRunnable> startables = new ArrayList<>();

            for (int i = 0; i < LIMIT; i++) {
                assertLessDuration(NO_DELAY_MILLIS, () -> {
                    StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
                    CompletableFuture<Integer> future = getFutureWithDelay(waitingRunnable);
                    assertThat(futureTracker.track(future, timeout)).isTrue();
                    startables.add(waitingRunnable);
                });
            }

            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            CompletableFuture<Integer> extraFuture = getFutureWithDelay(waitingRunnable);
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture, timeout)).isFalse());

            assertGreaterDuration(DELAY_MILLIS, () -> {
                startables.forEach(StartableRunnable::awaitReadiness);
                startables.forEach(StartableRunnable::start);

                assertThat(futureTracker.track(extraFuture, DELAY_DURATION.multipliedBy(2))).isTrue();
            });
        }

        @Test
        public void testExtraFutureWithoutTimeout() throws InterruptedException {
            List<StartableRunnable> startables = new ArrayList<>();

            for (int i = 0; i < LIMIT; i++) {
                assertLessDuration(NO_DELAY_MILLIS, () -> {
                    StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
                    CompletableFuture<Integer> future = getFutureWithDelay(waitingRunnable);
                    assertThat(futureTracker.track(future)).isTrue();
                    startables.add(waitingRunnable);
                });
            }

            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            CompletableFuture<Integer> extraFuture = getFutureWithDelay(waitingRunnable);
            CompletableFuture<Void> trackFuture = CompletableFuture.runAsync(() -> {
                try {
                    futureTracker.track(extraFuture);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });

            assertThrows(TimeoutException.class, () -> trackFuture.get(DELAY_MILLIS, TimeUnit.MILLISECONDS));
            assertThat(trackFuture.isDone()).isFalse();

            assertGreaterDuration(DELAY_MILLIS, () -> {
                startables.forEach(StartableRunnable::awaitReadiness);
                startables.forEach(StartableRunnable::start);

                try {
                    trackFuture.get(DELAY_MILLIS * 2, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                assertThat(trackFuture.isDone()).isTrue();
            });
        }

        @Test
        public void testExtraFutureWithDoneStatus() throws InterruptedException {
            for (int i = 0; i < LIMIT; i++) {
                assertLessDuration(NO_DELAY_MILLIS, () -> {
                    StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
                    CompletableFuture<Integer> future = getFutureWithDelay(waitingRunnable);
                    assertThat(futureTracker.track(future)).isTrue();
                });
            }

            assertThat(futureTracker.remaining()).isEqualTo(futureTracker.limit());

            CompletableFuture<Integer> extraFuture = CompletableFuture.completedFuture(1);
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture)).isTrue());
            assertLessDuration(NO_DELAY_MILLIS, () -> assertThat(futureTracker.track(extraFuture, DELAY_DURATION)).isTrue());

            assertThat(futureTracker.remaining()).isEqualTo(futureTracker.limit());
        }

        @Test
        public void testExtraFutureDuringStopping() throws InterruptedException {
            List<StartableRunnable> startables = new ArrayList<>();
            for (int i = 0; i < LIMIT; i++) {
                assertLessDuration(NO_DELAY_MILLIS, () -> {
                    StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
                    CompletableFuture<Integer> future = getFutureWithDelay(waitingRunnable);
                    assertThat(futureTracker.track(future)).isTrue();
                    waitingRunnable.awaitReadiness();
                    startables.add(waitingRunnable);
                });
            }

            StartableRunnable waitingRunnable = StartableRunnable.of(new SleepingRunnable(DELAY_MILLIS));
            CompletableFuture<Integer> extraFuture = getFutureWithDelay(waitingRunnable);

            StartableRunnable taskRunnable = StartableRunnable.of(() -> {
                try {
                    futureTracker.track(extraFuture);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            CompletableFuture<Integer> trackFuture = getFutureWithDelay(taskRunnable);
            taskRunnable.awaitReadiness();
            taskRunnable.start();

            startables.forEach(StartableRunnable::start);
            assertGreaterDuration(DELAY_MILLIS, () -> futureTracker.awaitRemaining(DELAY_DURATION));

            assertLessDuration(NO_DELAY_MILLIS, () -> {
                ExecutionException exception = assertThrows(ExecutionException.class, trackFuture::get);
                assertThat(exception.getMessage())
                        .isEqualTo("java.lang.IllegalStateException: Future tracker is disabled before tracking start");
                assertThat(trackFuture.isDone()).isTrue();
            });
            assertThat(futureTracker.remaining()).isEqualTo(0);
        }
    }
}
