# th2-tasks-utils (0.1.4)

This library includes classes for managing tasks.

## [FutureTracker](src/main/java/com/exactpro/th2/taskutils/FutureTracker.java) class

The class tracks futures and when needed tries to wait for them.

## [BlockingScheduledRetryableTaskQueue](src/main/java/com/exactpro/th2/taskutils/BlockingScheduledRetryableTaskQueue.java) class

Queue with maximum task capacity, maximum total task data capacity and retry scheduler. Queue does not allow new tasks to exceed capacity limitations.

Task extraction order is determined by their schedule time, that is task with earlier schedule time will be extracted before the task with later schedule time. For equal schedule times no particular order is guarantied.

Task extraction does not free up resources in queue. They are reserved for future retries. Resources are only released on task completion.

## Release notes

### 0.1.4

* The release includes BROKEN changes related to `FutureTracker` class:
  * Public constructors have been replaced to `create` factory methods.
  * The `boolean track(CompletableFuture<T> future)` method can return `false` and doesn't track feature when limit is positive. 

* Provided ability to limit capacity of `FutureTracker` class
* Added `FutureTracker.create()` and `FutureTracker.create(int limit)` factory methods
* Added `boolean track(CompletableFuture<T> future, long timeoutMillis)  throws InterruptedException` overload method in `FutureTracker` class.