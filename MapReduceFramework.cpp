#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h"
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>


// Define constants or macros if needed
#define UNDEFINED_PERCENTAGE -1.0f


// Define the JobState structure to hold job state information
struct ThreadContext {
    JobContext* jobContext;  // Points to the parent JobContext
    int threadId;            // Unique ID (0 to numThreads-1)
    IntermediateVec intermediates; // Local intermediate results

};

struct JobContext {
    // — Client & data pointers —
    const MapReduceClient& client;   // to call map() and reduce()
    const InputVec*    inputVec;     // pointer to the user’s input [(K1*,V1*)…]
          OutputVec*   outputVec;    // pointer to the global output [(K3*,V3*)…]
    const int          numThreads;   // # of worker threads to spawn

    // — Thread management —
    std::vector<std::thread> workers;    // the actual thread objects

    std::vector<ThreadContext> threadContexts;
    // one “emit‐ctx” pointer per thread (passed to emit2/emit3)

    // — Input‐claiming counter (map) —
    std::atomic<size_t>           nextInputIndex{0};
        // each thread does fetch_add(1) to grab the next (K1*,V1*)

    Barrier                        mapSortBarrier;

    // — Shuffle‐phase queue & coordination —
    std::mutex                     shuffleMutex;
    std::queue<IntermediateVec>    shuffleQueue;
        // thread 0 will push each key‐group here
    std::atomic<bool>              shuffleDone{false};
        // signals the other threads that no more groups will be enqueued

    // — Job state & progress counters (packed into one atomic) —
    std::atomic<uint64_t>          jobState{0};
        // e.g. bits 0–1 = stage enum (MAP, SORT, SHUFFLE, REDUCE)
        //      bits 2–32 = # completed in current stage
        //      bits 33–63 = # total in current stage

    // — (Optional) convenience counters —
    std::atomic<size_t>            totalIntermediates{0};  // bumped in emit2
    std::atomic<size_t>            totalReduceCalls{0};    // bumped in emit3

    // Static function to calculate the total number of tasks (bits 33–63)
    static uint32_t calculateTotal(uint64_t jobState) {
      return static_cast<uint32_t>((jobState >> 33) & 0x1FFFFFFFF); // Mask for 31 bits
    }

    // Static function to calculate the number of completed tasks (bits 2–32)
    static uint32_t calculateProgress(uint64_t jobState) {
      return static_cast<uint32_t>((jobState >> 2) & 0x1FFFFFFF); // Mask for 30 bits
    }

    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int numThreads)
        : client(client), inputVec(&inputVec), outputVec(&outputVec), numThreads(numThreads),
          nextInputIndex(0), shuffleDone(false), jobState(0), totalIntermediates(0), totalReduceCalls(0),
          mapSortBarrier(numThreads) {
      for (int i = 0; i < numThreads; ++i) {
        threadContexts[i] = ThreadContext{this, i, IntermediateVec()};
      }
      // Additional initialization logic if needed
    }
};


/**
 * Emits an intermediate key-value pair (K2, V2) during the map phase.
 *
 * This function is called by the `MapReduceClient` implementation during the map phase
 * to emit intermediate results. The emitted key-value pair is stored in a thread-local
 * context, which will later be used during the shuffle phase.
 *
 * @param key Pointer to the key of type K2. The key must remain valid until the end of the job.
 * @param value Pointer to the value of type V2. The value must remain valid until the end of the job.
 * @param context Context object for the MapReduce job, which manages thread-local storage
 *                for intermediate key-value pairs.
 */
void emit2(K2* key, V2* value, void* context);

/**
 * Emits a final key-value pair (K3, V3) during the reduce phase.
 *
 * This function is called by the `MapReduceClient` implementation during the reduce phase
 * to emit final results. The emitted key-value pair is added to the output vector provided
 * by the user.
 *
 * @param key Pointer to the key of type K3. The key must remain valid until the end of the job.
 * @param   value Pointer to the value of type V3. The value must remain valid until the end of the job.
 * @param context Context object for the MapReduce job, which manages the output vector
 *                for final key-value pairs.
 */
void emit3(K3* key, V3* value, void* context);

/**
 * Starts a MapReduce job with the given client, input, and output vectors.
 *
 * This function initializes and starts a MapReduce job. It creates the necessary threads
 * for the map, shuffle, and reduce phases, and manages the synchronization between them.
 * The function returns a handle to the job, which can be used to monitor its progress
 * or wait for its completion.
 *
 * @param client Reference to the `MapReduceClient` implementation, which defines the map
 *               and reduce logic.
 * @param inputVec Input vector containing key-value pairs (K1, V1) to be processed.
 * @param outputVec Output vector to store the final key-value pairs (K3, V3) produced
 *                  by the reduce phase.
 * @param multiThreadLevel Number of threads to use for the job. This determines the level
 *                         of parallelism for the map and reduce phases.
 * @return A handle to the MapReduce job, which can be used with other job-related functions.
 */
JobContext* startMapReduceJob(const MapReduceClient& client,
                              const InputVec& inputVec,
                              OutputVec& outputVec,
                              int multiThreadLevel) {
  auto jobContext = new (std::nothrow) JobContext(client, inputVec, outputVec, multiThreadLevel);
  if (!jobContext) {
    throw std::runtime_error("Failed to allocate JobContext");
  }

  try {
    for (int i = 0; i < multiThreadLevel; ++i) {
      jobContext->workers.emplace_back([jobContext, i]() {
          try {
            // Map phase
            mapPhase(jobContext, i);

            // Sort phase
            sortPhase(jobContext, i);

            // Barrier synchronization to enter shuffle phase
            jobContext->mapSortBarrier.arrive_and_wait();

            // Only thread 0 performs the shuffle phase
            if (i == 0) {
              shufflePhase(jobContext);
            }

            // Reduce phase
            reducePhase(jobContext, i);
          } catch (const std::exception& e) {
            std::cerr << "Error in thread " << i << ": " << e.what() << '\n';
          } catch (...) {
            std::cerr << "Unknown error occurred in thread " << i << '\n';
          }
      });
    }
  } catch (const std::system_error& e) {
    delete jobContext;
    throw std::runtime_error("Failed to create threads: " + std::string(e.what()));
  }

  return jobContext;
}


/**
 * Waits for the MapReduce job to complete.
 *
 * This function blocks the calling thread until the specified MapReduce job has completed
 * all its phases (map, shuffle, and reduce). It ensures that all resources associated
 * with the job are properly cleaned up before returning.
 *
 * @param job Handle to the MapReduce job. The handle must have been obtained from
 *            `startMapReduceJob`.
 */
void waitForJob(JobHandle job);

/**
 * Retrieves the current state of the MapReduce job.
 *
 * This function provides information about the current stage of the job (map, shuffle,
 * or reduce) and the progress percentage within that stage. The progress percentage
 * is a value between 0.0 and 100.0, or -1.0 if the job has not started.
 *
 * @param job Handle to the MapReduce job. The handle must have been obtained from
 *            `startMapReduceJob`.
 * @param state Pointer to a `JobState` structure, which will be populated with the
 *              current stage and progress percentage of the job.
 */
void getJobState(JobHandle job, JobState* state)
{
    auto jobContext = static_cast<JobContext*>(job);
    if (!jobContext) {
        sysErr("Invalid job handle");
    }

    // Lock the mutex to safely access the job state
    std::lock_guard<std::mutex> lock(jobContext->shuffleMutex);
    uint64_t atomicValue = jobContext->jobState.load();
    uint32_t total = JobContext::calculateTotal(atomicValue);
    uint32_t progress = JobContext::calculateProgress(atomicValue);

    if (total == 0) {
        state->percentage = UNDEFINED_PERCENTAGE;
    } else {
        state->percentage = 100 * (static_cast<float>(progress) / static_cast<float>(total));
    }
    state->stage = JobContext::calculateStage(atomicValue);
}

/**
 * Closes the MapReduce job handle and releases associated resources.
 *
 * This function cleans up all resources associated with the specified MapReduce job.
 * It must be called after the job has completed and is no longer needed. After calling
 * this function, the job handle becomes invalid and must not be used again.
 *
 * @param job Handle to the MapReduce job. The handle must have been obtained from
 *            `startMapReduceJob`.
 */
void closeJobHandle(JobHandle job);