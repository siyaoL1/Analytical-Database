#ifndef THREADPOOL_H
#define THREADPOOL_H

/**
 * @file threadpool.h
 * @brief Threadpool Header File
 */

 /**
 * Increase this constants at your own risk
 * Large values might slow down your system
 */
#define MAX_THREADS 64
#define MAX_QUEUE 65536


typedef enum {
    immediate_shutdown = 1,
    graceful_shutdown  = 2
} threadpool_shutdown_t;

/**
 *  @struct threadpool_task
 *  @brief the work struct
 *
 *  @var function Pointer to the function that will perform the task.
 *  @var argument Argument to be passed to the function.
 */
/**
 * Definition of a task in thread pool
 */
typedef struct {
    void (*function)(void *);
    void *argument;
} threadpool_task_t;


/**
 * @struct threadpool
 * @brief The threadpool struct
 * Structural Definition of Thread Pool
 *  @var lock         Mutex Locks for Internal Work
 *  @var notify       Conditional variables for interthread notifications
 *  @var threads      Thread array, represented here by pointer, array name = first element pointer
 *  @var thread_count Number of threads
 *  @var queue        An array of stored tasks, namely task queues
 *  @var queue_size   Task queue size
 *  @var head         The first task position in the task queue (Note: All tasks in the task queue are not running)
 *  @var tail         Next location of the last task in the task queue (note: queues are stored in arrays, and head and tail indicate queue location)
 *  @var count        The number of tasks in the task queue, that is, the number of tasks waiting to run
 *  @var shutdown     Indicates whether the thread pool is closed
 *  @var started      Number of threads started
 */
typedef struct threadpool_t {
  pthread_mutex_t lock;     /* mutex */
  pthread_cond_t notify;    /* Conditional variable */
  pthread_t *threads;       /* Starting Pointer of Thread Array */
  threadpool_task_t *queue; /* Starting Pointer of Task Queue Array */
  int thread_count;         /* Number of threads */
  int queue_size;           /* Task queue length */
  int head;                 /* Current task queue head */
  int tail;                 /* End of current task queue */
  int count;                /* Number of tasks currently to be run */
  int shutdown;             /* Is the current state of the thread pool closed? */
  int started;              /* Number of threads running */
} threadpool_t;

/* Define error codes */
typedef enum {
    threadpool_invalid        = -1,
    threadpool_lock_failure   = -2,
    threadpool_queue_full     = -3,
    threadpool_shutdown       = -4,
    threadpool_thread_failure = -5
} threadpool_error_t;

typedef enum {
    threadpool_graceful       = 1
} threadpool_destroy_flags_t;

/* Here are three external API s for thread pool */

/**
 * @function threadpool_create
 * @brief Creates a threadpool_t object.
 * @param thread_count Number of worker threads.
 * @param queue_size   Size of the queue.
 * @param flags        Unused parameter.
 * @return a newly created thread pool or NULL
 */
/**
 * Create a thread pool with thread_count threads and queue_size task queues. The flags parameter is not used
 */
threadpool_t *threadpool_create(int thread_count, int queue_size, int flags);

/**
 * @function threadpool_add
 * @brief add a new task in the queue of a thread pool
 * @param pool     Thread pool to which add the task.
 * @param function Pointer to the function that will perform the task.
 * @param argument Argument to be passed to the function.
 * @param flags    Unused parameter.
 * @return 0 if all goes well, negative values in case of error (@see
 * threadpool_error_t for codes).
 */
/**
 *  Add tasks to thread pool, pool is thread pool pointer, routine is function pointer, arg is function parameter, flags is not used
 */
int threadpool_add(threadpool_t *pool, void (*routine)(void *),
                   void *arg, int flags);

/**
 * @function threadpool_destroy
 * @brief Stops and destroys a thread pool.
 * @param pool  Thread pool to destroy.
 * @param flags Flags for shutdown
 *
 * Known values for flags are 0 (default) and threadpool_graceful in
 * which case the thread pool doesn't accept any new tasks but
 * processes all pending tasks before shutdown.
 */
/**
 * Destroy thread pools, flags can be used to specify how to close them
 */
int threadpool_destroy(threadpool_t *pool, int flags);

/**
 * @function void *threadpool_thread(void *threadpool)
 * @brief the worker thread
 * @param threadpool the pool which own the thread
 */
/**
 * Functions in the thread pool where each thread is running
 * Declare that static should only be used to make functions valid in this file
 */
void *threadpool_thread(void *threadpool);

int threadpool_free(threadpool_t *pool);



#endif /* THREADPOOL_H */