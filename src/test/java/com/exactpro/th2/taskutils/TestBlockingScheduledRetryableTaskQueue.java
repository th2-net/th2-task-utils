/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.taskutils;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

public class TestBlockingScheduledRetryableTaskQueue {

    public static final long START_TIME = 1278417364718L;

    @Test
    public void testBasicLogic() throws InterruptedException {

        final int  maxTaskCount = 35;
        final long maxDataSize = 23748987;
        final long dataSize = 1489237;
        ScheduledRetryableTask<Object> task = new ScheduledRetryableTask<>(START_TIME, 1, dataSize, null);
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(maxTaskCount, maxDataSize, null);

        // first submission
        queue.submit(task);
        assertEquals(maxTaskCount, queue.getMaxTaskCount(), "Max task count");
        assertEquals(maxDataSize, queue.getMaxDataSize(), "Max data count");
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(1, queue.getQueuedTaskCount(), "Queued task count");
        assertEquals(dataSize, queue.getUsedDataSize(), "Used data size");

        // first extraction
        ScheduledRetryableTask<Object> extractedTask1 = queue.take();
        assertEquals(task, extractedTask1, "Extracted task");
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(0, queue.getQueuedTaskCount(), "Queued task count");
        assertEquals(dataSize, queue.getUsedDataSize(), "Used data size");

        // resubmission
        queue.retry(task);
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(1, queue.getQueuedTaskCount(), "Queued task count");
        assertEquals(dataSize, queue.getUsedDataSize(), "Used data size");

        // second extraction
        ScheduledRetryableTask<Object> retriedTask = queue.take();
        assertEquals(task, retriedTask.getParentTask(), "Retried task parent");
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(0, queue.getQueuedTaskCount(), "Queued task count");
        assertEquals(dataSize, queue.getUsedDataSize(), "Used data size");

        // completion
        queue.complete(retriedTask);
        assertEquals(0, queue.getTaskCount(), "Task count");
        assertEquals(0, queue.getQueuedTaskCount(), "Queued task count");
        assertEquals(0, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testJobExtractionOrder() throws InterruptedException {

        ScheduledRetryableTask<Object> task1 = new ScheduledRetryableTask<>(START_TIME, 1, 100, null);
        ScheduledRetryableTask<Object> task2 = new ScheduledRetryableTask<>(START_TIME + 1234, 2, 100, null);
        ScheduledRetryableTask<Object> task3 = new ScheduledRetryableTask<>(START_TIME + 5678, 3, 100, null);
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(Integer.MAX_VALUE, Long.MAX_VALUE, null);
        queue.submit(task2);
        queue.submit(task3);
        queue.submit(task1);

        assertEquals(task1, queue.take());
        assertEquals(task2, queue.take());
        assertEquals(task3, queue.take());
    }

    @Test
    public void testBlockingByCount() throws InterruptedException {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(2, Long.MAX_VALUE, null);

        final int taskCount = 10;
        StartableRunnable fastProducer = StartableRunnable.of(() -> {
            for (int i = 0; i < taskCount; i++) {
                submitUnchecked(queue, new ScheduledRetryableTask<>(System.nanoTime(), 0, i * 10, null));
                pause(1);
            }
        });

        StartableRunnable slowConsumer = StartableRunnable.of(() -> {
            for (int i = 0; i < taskCount; i++) {
                try {
                    ScheduledRetryableTask<Object> task = queue.awaitScheduled();
                    assertEquals(i * 10, task.getPayloadSize(), "Incorrect task");
                    pause(20);
                    queue.complete(task);
                } catch (InterruptedException ignored) {
                }
            }
        });

        Thread producerThread = new Thread(fastProducer);
        Thread consumerThread = new Thread(slowConsumer);

        producerThread.start();
        consumerThread.start();

        fastProducer.awaitReadiness();
        slowConsumer.awaitReadiness();

        slowConsumer.start();
        pause(50);
        fastProducer.start();

        producerThread.join();
        consumerThread.join();

        assertEquals(0, queue.getTaskCount(), "Task count after execution");
        assertEquals(0, queue.getUsedDataSize(), "Data size after execution");
    }


    @Test
    public void testBlockingBySize() throws InterruptedException {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(Integer.MAX_VALUE, 100, null);

        final int taskCount = 10;
        StartableRunnable fastProducer = StartableRunnable.of(() -> {
            for (int i = 0; i < taskCount; i++) {
                submitUnchecked(queue, new ScheduledRetryableTask<>(System.nanoTime(), i, 80, null));
                pause(1);
            }
        });

        StartableRunnable slowConsumer = StartableRunnable.of(() -> {
            for (int i = 0; i < taskCount; i++) {
                try {
                    ScheduledRetryableTask<Object> task = queue.awaitScheduled();
                    assertEquals(i, task.getRetriesLeft(), "Incorrect task");
                    pause(20);
                    queue.complete(task);
                } catch (InterruptedException ignored) {
                }
            }
        });

        Thread producerThread = new Thread(fastProducer);
        Thread consumerThread = new Thread(slowConsumer);

        producerThread.start();
        consumerThread.start();

        fastProducer.awaitReadiness();
        slowConsumer.awaitReadiness();

        slowConsumer.start();
        pause(50);
        fastProducer.start();

        producerThread.join();
        consumerThread.join();

        assertEquals(0, queue.getTaskCount(), "Task count after execution");
        assertEquals(0, queue.getUsedDataSize(), "Data size after execution");
    }

    @Test
    public void testOversizedTaskIsAcceptedByEmptyQueue() {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(10, 100, null);
        ScheduledRetryableTask<Object> task = new ScheduledRetryableTask<>(System.nanoTime(), 1, 101, new Object());

        assertTimeoutPreemptively(TIMEOUT, () -> queue.submit(task), "Oversized task submission to empty queue");
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(101, queue.getUsedDataSize(), "Used data size");

        assertEquals(task, queue.take(), "Extracted task");
        queue.complete(task);
        assertEquals(0, queue.getTaskCount(), "Task count");
        assertEquals(0, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testOversizedTaskWaitsForEmptyQueue() throws Exception {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(10, 100, null);
        ScheduledRetryableTask<Object> smallTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 10, null);
        ScheduledRetryableTask<Object> oversizedTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 150, null);
        queue.submit(smallTask);

        CompletableFuture<Void> submission = submitAsync(queue, oversizedTask);
        assertBlocked(submission, "Oversized task submission to non-empty queue");
        assertEquals(1, queue.getTaskCount(), "Task count");

        queue.complete(queue.take());
        submission.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(150, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testOversizedTaskBlocksFollowingSubmissions() throws Exception {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(10, 100, null);
        ScheduledRetryableTask<Object> oversizedTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 150, null);
        ScheduledRetryableTask<Object> smallTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 10, null);
        assertTimeoutPreemptively(TIMEOUT, () -> queue.submit(oversizedTask), "Oversized task submission to empty queue");

        CompletableFuture<Void> submission = submitAsync(queue, smallTask);
        assertBlocked(submission, "Task submission while oversized task occupies queue");

        queue.complete(queue.take());
        submission.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(10, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testSubmitAfterMaxDataSizeDecreasedBelowTaskSize() throws Exception {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(10, 100, null);
        ScheduledRetryableTask<Object> firstTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 80, null);
        ScheduledRetryableTask<Object> secondTask = new ScheduledRetryableTask<>(System.nanoTime(), 1, 80, null);
        queue.submit(firstTask);

        CompletableFuture<Void> submission = submitAsync(queue, secondTask);
        assertBlocked(submission, "Task submission to full queue");

        queue.setMaxDataSize(50);
        queue.complete(queue.take());
        submission.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(80, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testIncreasingMaxDataSizeUnblocksSubmit() throws Exception {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(10, 100, null);
        queue.submit(new ScheduledRetryableTask<>(System.nanoTime(), 1, 80, null));

        CompletableFuture<Void> submission = submitAsync(queue, new ScheduledRetryableTask<>(System.nanoTime(), 1, 80, null));
        assertBlocked(submission, "Task submission to full queue");

        queue.setMaxDataSize(200);
        submission.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals(2, queue.getTaskCount(), "Task count");
        assertEquals(160, queue.getUsedDataSize(), "Used data size");
    }

    @Test
    public void testBlockedSubmitCanBeInterrupted() throws Exception {
        BlockingScheduledRetryableTaskQueue<Object> queue = new BlockingScheduledRetryableTaskQueue<>(1, 100, null);
        queue.submit(new ScheduledRetryableTask<>(System.nanoTime(), 1, 10, null));

        CompletableFuture<Throwable> result = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                queue.submit(new ScheduledRetryableTask<>(System.nanoTime(), 1, 10, null));
                result.complete(null);
            } catch (Throwable e) {
                result.complete(e);
            }
        });
        thread.setDaemon(true);
        thread.start();

        assertBlocked(result, "Task submission to full queue");
        thread.interrupt();
        assertInstanceOf(InterruptedException.class, result.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                "Blocked submit must be interrupted");
        assertEquals(1, queue.getTaskCount(), "Task count");
        assertEquals(10, queue.getUsedDataSize(), "Used data size");
    }

    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final long BLOCKING_CHECK_MILLIS = 200;

    private static CompletableFuture<Void> submitAsync(BlockingScheduledRetryableTaskQueue<Object> queue,
                                                       ScheduledRetryableTask<Object> task) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                queue.submit(task);
                future.complete(null);
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
        });
        thread.setDaemon(true);
        thread.start();
        return future;
    }

    private static void assertBlocked(CompletableFuture<?> future, String message) {
        assertThrows(TimeoutException.class,
                () -> future.get(BLOCKING_CHECK_MILLIS, TimeUnit.MILLISECONDS),
                message + " must block");
        assertFalse(future.isDone(), message + " must block");
    }

    private static void submitUnchecked(BlockingScheduledRetryableTaskQueue<Object> queue,
                                        ScheduledRetryableTask<Object> task) {
        try {
            queue.submit(task);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {

        }
    }
}