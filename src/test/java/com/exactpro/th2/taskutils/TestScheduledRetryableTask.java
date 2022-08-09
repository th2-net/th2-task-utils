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

public class TestScheduledRetryableTask {
    private static final long SCHEDULE_TIME = 1234134343;
    @Test
    public void testNonRetryable() {
        ScheduledRetryableTask<Object> task = new ScheduledRetryableTask<>(SCHEDULE_TIME, 0, 10, null);
        assertEquals(SCHEDULE_TIME, task.getScheduledTime(), "Task scheduled time");
        assertEquals(0, task.getRetriesLeft(), "Retries left");
        assertEquals(0, task.getRetriesDone(), "Retries done");
    }

    @Test
    public void testRetryable() {
        ScheduledRetryableTask<Object> task = new ScheduledRetryableTask<>(SCHEDULE_TIME, 3, 10, null);
        assertEquals(SCHEDULE_TIME, task.getScheduledTime(), "Task scheduled time");
        assertEquals(3, task.getRetriesLeft(), "Retries left");
        assertEquals(0, task.getRetriesDone(), "Retries done");
    }

    @Test
    public void testRetryOf() {
        ScheduledRetryableTask<Object> task = new ScheduledRetryableTask<>(SCHEDULE_TIME, 5, 10, null);

        long newScheduleTime1 = SCHEDULE_TIME + 1213;
        ScheduledRetryableTask<Object> retriedTask1 = ScheduledRetryableTask.retryOf(task, newScheduleTime1);

        assertEquals(task, retriedTask1.getParentTask(), "Parent task");
        assertEquals(newScheduleTime1, retriedTask1.getScheduledTime(), "Task scheduled time");
        assertEquals(4, retriedTask1.getRetriesLeft(), "Retries left");
        assertEquals(1, retriedTask1.getRetriesDone(), "Retries done");

        long newScheduleTime2 = SCHEDULE_TIME + 3456;
        ScheduledRetryableTask<Object> retriedTask2 = ScheduledRetryableTask.retryOf(retriedTask1, newScheduleTime2);

        assertEquals(retriedTask1, retriedTask2.getParentTask(), "Parent task");
        assertEquals(newScheduleTime2, retriedTask2.getScheduledTime(), "Task scheduled time");
        assertEquals(3, retriedTask2.getRetriesLeft(), "Retries left");
        assertEquals(2, retriedTask2.getRetriesDone(), "Retries done");
    }

    @Test
    public void testOrder() {
        ScheduledRetryableTask<Object> task1 = new ScheduledRetryableTask<>(SCHEDULE_TIME, 5, 10, null);
        ScheduledRetryableTask<Object> task2 = new ScheduledRetryableTask<>(SCHEDULE_TIME + 1234, 6, 7, null);

        assertEquals(-1, ScheduledRetryableTask.compareOrder(task1, task2));
        assertEquals(0, ScheduledRetryableTask.compareOrder(task1, task1));
        assertEquals(1, ScheduledRetryableTask.compareOrder(task2, task1));
    }
}
