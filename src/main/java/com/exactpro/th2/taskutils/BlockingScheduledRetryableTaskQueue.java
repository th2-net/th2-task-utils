/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingScheduledRetryableTaskQueue<V> {

    private volatile long maxDataSize;
    private final int maxTaskCount;
    private final RetryScheduler scheduler;
    private final Queue<ScheduledRetryableTask<V>> taskQueue;
    private final Set<ScheduledRetryableTask<V>> taskSet;

    private final AtomicLong dataSize;
    private final Lock lock;
    private final Condition addition;
    private final Condition removal;

    /**
     * <P>
     * Creates new queue with given maximum task capacity, maximum total task data capacity and retry scheduler.
     * Queue does not allow new tasks to exceed capacity limitations,
     * </P>
     * <P>
     * Task extraction order is determined by their schedule time, that is task with earlier schedule time
     * will be extracted before the task with later schedule time. For equal schedule times no particular order is guarantied
     *</P>
     * <P>
     * Task extraction does not free up resources in queue. They are reserved for future retries. Resources are only
     * released on task completion.
     *</P>
     * @param maxTaskCount  Maximum number of tasks that can be submitted before blocking.
     *                      This parameter has effect on submit() method which will be blocked if capacity
     *                      limitations are reached.
     * @param maxDataSize   Maximum cumulative data size of all jobs in the queue before blocking.
     *                      This parameter has effect on submit() method which will be blocked if capacity
     *                      limitations are reached.
     * @param scheduler     RetryScheduler that is called to compute retry delay time when submitting job for retry.
     *                      In case of <I>null</I> value is provided, 0 delay will be used.
     *
     */
    public BlockingScheduledRetryableTaskQueue(int maxTaskCount, long maxDataSize, RetryScheduler scheduler) {
        this.maxTaskCount = maxTaskCount;
        this.maxDataSize = maxDataSize;
        this.scheduler = (scheduler != null) ? scheduler : (unused) -> 0;

        taskQueue = new PriorityQueue<>(ScheduledRetryableTask::compareOrder);
        taskSet = new HashSet<>();
        dataSize = new AtomicLong(0);
        lock = new ReentrantLock();
        addition = lock.newCondition();
        removal = lock.newCondition();
    }


    /**
     * Submits task to the queue.<BR>
     * Blocks until available capacity requirements are met, that is queue has space for this task by
     * the count and cumulative data size of submitted task
     *
     * @param task   Task to be added to the queue
     */
    public void submit(ScheduledRetryableTask<V> task) {
        lock.lock();
        try {
            if (taskSet.contains(task))
                throw new IllegalStateException("Task has been already submitted");

            while (true) {
                long capacityLeft = maxDataSize - dataSize.get();
                if (capacityLeft >= task.getPayloadSize() && taskSet.size() < maxTaskCount) {
                    dataSize.addAndGet(task.getPayloadSize());
                    addTask(task);
                    break;
                } else {
                    removal.awaitUninterruptibly();
                }
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * Submits task for retry to the queue.<BR>
     * New task object will be created with schedule time calculated by RetryScheduler provided when queue was created.
     * If task retry limit was reached than exception will be thrown.<BR>
     *
     * This method does not block and does not alter queue capacity limitations.
     *
     * @param task   Task to be resubmitted to the queue
     */
    public void retry(ScheduledRetryableTask<V> task) {
        ScheduledRetryableTask<V> retriedTask = ScheduledRetryableTask.retryOf(
                task,
                System.nanoTime() + scheduler.nextRetry(task.getRetriesDone()));

        lock.lock();
        try {
            if (!taskSet.contains(task))
                throw new IllegalStateException("Task to retry has not been submitted previously");
            taskSet.remove(task);
            addTask(retriedTask);
        } finally {
            lock.unlock();
        }
    }


    /**
     * Completes task and releases capacity resources associated with it.<BR>
     * If task was retried, retried task object returned by retry() method must be passed to this function
     *
     * @param task   Task for which resources to be released
     */
    public void complete(ScheduledRetryableTask<V> task) {
        lock.lock();
        try {
            if (!taskSet.contains(task))
                throw new IllegalStateException("Task to complete has not been submitted previously");
            taskSet.remove(task);

            dataSize.addAndGet(-task.getPayloadSize());
            removal.signalAll();
        } finally {
            lock.unlock();
        }
    }


    /**<P>
     *     Returns task with earliest schedule time. Blocks until any task is available in the queue.<BR>
     *     This method does not wait for task's schedule time, so it might return task that is scheduled
     *     in the future. Use <I>awaitScheduled()</I> method to wait until there is actual task that can be executed
     *     according the schedule.
     * </P>
     * <P>
     *     No order is guarantied if multiple tasks have same schedule time. Capacities reserved by this task are not
     *     released until this task is submitted to complete() method.
     *</P>
     * @return  Task with the earliest schedule time.
     */
    public ScheduledRetryableTask<V> take() {
        lock.lock();
        try {
            while (true) {
                if (!taskQueue.isEmpty())
                    return taskQueue.poll();
                else
                    addition.awaitUninterruptibly();
            }
        } finally {
            lock.unlock();
        }
    }



    /**
     * <P>
     *     Waits and returns task with earliest schedule time that can be immediately executed. Blocks until such task
     *     is available.This method will block even when queue is not empty, if all tasks are scheduled for execution
     *     in future.
     * </P>
     * <P>
     *     No order is guarantied if multiple tasks have same schedule time. Capacities reserved by this task are not
     *     released until this task is submitted to complete() method.
     *</P>
     *
     * @return  Task with the earliest schedule time.
     * @throws InterruptedException if method call was interrupted
     */
    public ScheduledRetryableTask<V> awaitScheduled() throws InterruptedException {
        lock.lock();
        try {
            while (true) {
                if (taskQueue.isEmpty())
                    addition.await();
                else {
                    ScheduledRetryableTask<V> job = taskQueue.peek();
                    long now = System.nanoTime();
                    if (now < job.getScheduledTime())
                        addition.awaitNanos(job.getScheduledTime() - now);
                    else
                        return taskQueue.poll();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void addTask(ScheduledRetryableTask<V> task) {
        taskQueue.offer(task);
        taskSet.add(task);
        addition.signalAll();
    }

    /**
     * @return Maximum number of uncompleted tasks that can be submitted before blocking
     */
    public int getMaxTaskCount() {
        return maxTaskCount;
    }

    /**
     * @return Current number of tasks in the queue
     */
    public int getTaskCount() {
        lock.lock();
        try {
            return taskSet.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return Current number of tasks in the queue that are not extracted yet
     */
    public int getQueuedTaskCount() {
        lock.lock();
        try {
            return taskQueue.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return Maximum cumulative data size for all tasks that can be submitted before blocking
     */
    public long getMaxDataSize() {
        lock.lock();
        try {
            return maxDataSize;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Sets cumulative data size for all tasks that can be submitted before blocking
     * @param value  New data size to be set
     */
    public void setMaxDataSize(long value) {
        lock.lock();
        try {
            this.maxDataSize = value;
        } finally {
            lock.unlock();
        }
    }

    /**
     * @return Current cumulative data size of submitted tasks
     */
    public long getUsedDataSize() {
        lock.lock();
        try {
            return dataSize.get();
        } finally {
            lock.unlock();
        }
    }
}
