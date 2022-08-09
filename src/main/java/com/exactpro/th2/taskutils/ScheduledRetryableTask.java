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

public class ScheduledRetryableTask<V> {
    private final long scheduledTime;
    private final int  retriesLeft;
    private final int  retriesDone;
    private final long payloadSize;
    private final V    payload;
    private final ScheduledRetryableTask<V> parentTask;

    private ScheduledRetryableTask(long scheduledTime,
                                   int maxRetries,
                                   int retriesDone,
                                   long payloadSize,
                                   V payload,
                                   ScheduledRetryableTask<V> parentTask) {
        if (maxRetries < 0)
            throw new IllegalArgumentException("Number of max retries can not be negative");

        if (payloadSize < 0)
            throw new IllegalArgumentException("Payload size can not be negative");

        this.scheduledTime = scheduledTime;
        this.retriesLeft = maxRetries;
        this.retriesDone = retriesDone;
        this.payloadSize = payloadSize;
        this.payload = payload;
        this.parentTask = parentTask;
    }


    public ScheduledRetryableTask(long scheduledTime, int maxRetries, long payloadSize, V payload) {
        this(scheduledTime, maxRetries, 0, payloadSize, payload, null);
    }

    public static<V> ScheduledRetryableTask<V> retryOf(ScheduledRetryableTask<V> task, long scheduledTime) {
        if (task.retriesLeft == 0)
            throw new IllegalStateException("Retry limit exceeded");

        return new ScheduledRetryableTask<>(scheduledTime,
                task.retriesLeft - 1,
                task.retriesDone + 1,
                task.payloadSize,
                task.payload,
                task);
    }


    public int getRetriesLeft() {
        return retriesLeft;
    }

    public long getScheduledTime() {
        return scheduledTime;
    }

    public int getRetriesDone() {
        return retriesDone;
    }

    public long getPayloadSize() {
        return payloadSize;
    }

    public V getPayload() {
        return payload;
    }

    public ScheduledRetryableTask<V> getParentTask() {
        return parentTask;
    }

    public static<V> int compareOrder(ScheduledRetryableTask<V> task1, ScheduledRetryableTask<V> task2) {
        return Long.compare(task1.scheduledTime, task2.scheduledTime);
    }

    @Override
    public String toString() {
        return String.format("%s {scheduledTime: %d, retriesLeft: %d, retriesDone: %d, payloadSize: %d, payload: %s}",
                ScheduledRetryableTask.class.getSimpleName(),
                scheduledTime,
                retriesLeft,
                retriesDone,
                payloadSize,
                payload);
    }
}
