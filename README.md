# th2-tasks-utils (0.1.5)

This library includes classes for managing tasks.

## [FutureTracker](src/main/java/com/exactpro/th2/taskutils/FutureTracker.java) class

The class tracks futures and when needed tries to wait for them.

## [BlockingScheduledRetryableTaskQueue](src/main/java/com/exactpro/th2/taskutils/BlockingScheduledRetryableTaskQueue.java) class

Queue with maximum task capacity, maximum total task data capacity and retry scheduler. Queue does not allow new tasks to exceed capacity limitations.

Task extraction order is determined by their schedule time, that is task with earlier schedule time will be extracted before the task with later schedule time. For equal schedule times no particular order is guarantied.

Task extraction does not free up resources in queue. They are reserved for future retries. Resources are only released on task completion.

A task whose payload size exceeds the maximum data size is accepted only when the queue holds no data. It then occupies the whole data capacity until it is completed, and other submissions wait for it.

## Release notes

### 0.1.5

* The release includes BROKEN changes related to `BlockingScheduledRetryableTaskQueue` class:
  * The `void submit(ScheduledRetryableTask<V> task)` method can throw InterruptedException. A blocked submission can now be interrupted, for example on shutdown.

* Fixed: `BlockingScheduledRetryableTaskQueue.submit()` blocked forever when task's payload size exceeded max data size ([#53](https://github.com/th2-net/th2-task-utils/issues/53)).
  Such task is now accepted when the queue holds no data and occupies the whole data capacity until it is completed.
* `BlockingScheduledRetryableTaskQueue.setMaxDataSize()` wakes up blocked `submit()` calls so that they re-check capacity against the new value.

* Updated:
  * th2 gradle plugin: `0.3.15` (from `0.2.4`)
  * junit-bom: `5.14.4` (from `5.12.1`)
  * assertj-core: `3.27.7` (from `3.27.3`)

### 0.1.4

* The release includes BROKEN changes related to `FutureTracker` class:
  * The `boolean track(CompletableFuture<T> future)` method can throw InterruptedException.
  * The `void awaitRemaining()` and `awaitRemaining(long timeoutMillis)` methods can throw InterruptedException.

* Provided ability to limit capacity of `FutureTracker` class
* Added `FutureTracker.createUnlimited()` and `FutureTracker.create(int limit)` factory methods
* Added `boolean track(CompletableFuture<T> future, Duration timeout)` overload method in `FutureTracker` class.
* Added `void awaitRemaining(Duration timeout)` overload method in `FutureTracker` class.
* Added `int limit()` method into `FutureTracker` class.
* Added `boolean hasLimit()` method into `FutureTracker` class.
* Added `boolean isEnabled()` method into `FutureTracker` class.