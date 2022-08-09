/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBlockingScheduledRetryableTaskQueue {

    public static final long START_TIME = 1278417364718L;

    @Test
    public void testBasicLogic() {

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
    public void testJobExtractionOrder() {

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
                queue.submit(new ScheduledRetryableTask<>(System.nanoTime(), 0, i * 10, null));
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
                } catch (InterruptedException e) {
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
                queue.submit(new ScheduledRetryableTask<>(System.nanoTime(), i, 80, null));
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
                } catch (InterruptedException e) {
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

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {

        }
    }
}