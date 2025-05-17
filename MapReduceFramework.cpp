#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <algorithm>

// Define constants or macros if needed
#define UNDEFINED_PERCENTAGE -1.0f

struct JobContext {
    // — Client & data pointers —
    const MapReduceClient& client;   // to call map() and reduce()
    const InputVec* inputVec;        // pointer to the user’s input [(K1*,V1*)…]
    OutputVec* outputVec;            // pointer to the global output [(K3*,V3*)…]
    const int numThreads;            // # of worker threads to spawn

    // — Thread management —
    std::vector<std::thread> workers;    // the actual thread objects

    // — Thread-specific data —
    std::vector<IntermediateVec> intermediates; // One intermediate vector per thread

    // — Input-claiming counter (map) —
    std::atomic<size_t> nextInputIndex{0};

    // — Shuffle-phase queue & coordination —
    std::mutex shuffleMutex;
    std::queue<IntermediateVec> shuffleQueue;
    std::atomic<uint64_t> queueSize{0}; // Number of groups in the shuffle queue
    std::atomic<bool> shuffleDone{false};

    // — Job state & progress counters —
    std::atomic<uint64_t> jobState{0};

    // — Barrier for synchronization —
    Barrier mapSortBarrier;

    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int numThreads)
        : client(client), inputVec(&inputVec), outputVec(&outputVec), numThreads(numThreads),
          intermediates(numThreads), mapSortBarrier(numThreads) {
    }

    // Static function to calculate the total number of tasks (bits 33–63)
    static uint32_t calculateTotal(uint64_t jobState) {
      return static_cast<uint32_t>((jobState >> 33) & 0x1FFFFFFF); // Mask for 31 bits
    }

    // Static function to calculate the number of completed tasks (bits 2–32)
    static uint32_t calculateProgress(uint64_t jobState) {
      return static_cast<uint32_t>((jobState >> 2) & 0x1FFFFFFF); // Mask for 30 bits
    }
    // Static function to calculate the current stage (bits 0–1)
    static stage_t calculateStage(uint64_t jobState) {
      return static_cast<stage_t>(jobState & 0x3); // Mask for 2 bits
    }
};

// check equality if there isnt an operator== defined
auto keysEqual = [](const K2* a, const K2* b){
    return !(*a < *b) && !(*b < *a);
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
void mapPhase(JobContext* jobContext, int threadID) {
    auto& client = jobContext->client;
    auto& inputVec = *jobContext->inputVec;
    auto& intermediateVec = jobContext->intermediates[threadID];

    for (size_t i = 0; i < inputVec.size(); ++i) {
        // Claim the next input index
        size_t inputIndex = jobContext->nextInputIndex.fetch_add(1);
        if (inputIndex >= inputVec.size()) {
            break; // No more inputs to process
        }

        // Call the map function
        client.map(inputVec[inputIndex].first, inputVec[inputIndex].second, &intermediateVec);
        jobContext->jobState.fetch_add(1ULL << 2, std::memory_order_acq_rel);
        // Increment the progress counter
    }
}

void emit2(K2* key, V2* value, void* context)
{
  // Cast the context to an IntermediateVec pointer
  auto intermediateVec = static_cast<IntermediateVec*>(context);
  if (!intermediateVec) {
    std::fprintf(stderr, "system error: invalid intermediate vector context\n");
    std::exit (1);
  }

  // Add the key-value pair to the intermediate vector
  intermediateVec->emplace_back(key, value);
}

void sortPhase(JobContext* jobContext, int threadID) {
    auto& intermediateVec = jobContext->intermediates[threadID];

    // Sort the intermediate vector
    std::sort(intermediateVec.begin(), intermediateVec.end(),
              [](const IntermediatePair& a, const IntermediatePair& b) {
                  return *(a.first) < *(b.first);
              });

    // Barrier synchronization to enter shuffle phase
    jobContext->mapSortBarrier.barrier();
}

void shufflePhase(JobContext* JobContext) {
  // 1) Keep going until all per-thread vectors are drained
  while (true) {
    // 1a) Find the next key to process:
    K2* currentKey = nullptr;
    for (int t = 0; t < JobContext->numThreads; ++t) {
      auto& vec = JobContext->intermediates[t];
      if (!vec.empty()) {
        K2* candidate = vec.back().first;
        if (!currentKey || *candidate < *currentKey) {
          currentKey = candidate;
        }
      }
    }
    if (!currentKey) break;  // done all keys

    // 1b) Peel off _all_ pairs == currentKey from each thread’s vector
    IntermediateVec group;
    for (int t = 0; t < JobContext->numThreads; ++t) {
      auto& vec = JobContext->intermediates[t];
      while (!vec.empty() && keysEqual(vec.back().first, currentKey)) {
          group.push_back(vec.back());
          vec.pop_back();
      }
    }

    // 1c) Push that group onto the shared queue

    std::lock_guard<std::mutex> lg(JobContext->shuffleMutex);
    JobContext->shuffleQueue.push(std::move(group));

    JobContext->jobState.fetch_add(1ULL << 2, std::memory_order_acq_rel); // increment the progress counter

    JobContext->queueSize.fetch_add(1);  // your atomic counter for number of groups
  }

  // 2) Signal “no more groups”
  JobContext->shuffleDone.store(true, std::memory_order_release);
}

void reducePhase(JobContext* jobContext, int threadID) {
    auto& client = jobContext->client;
    auto& outputVec = *jobContext->outputVec;
    auto& shuffleQueue = jobContext->shuffleQueue;

    while (true) {
        IntermediateVec group;

        // Lock the mutex to safely access the shuffle queue
        {
            std::lock_guard<std::mutex> lg(jobContext->shuffleMutex);
            if (shuffleQueue.empty() && jobContext->shuffleDone.load()) {
                break; // No more groups to process
            }
            if (!shuffleQueue.empty()) {
                group = std::move(shuffleQueue.front());
                shuffleQueue.pop();
            }
        }

        // If we have a group, call the reduce function
        if (!group.empty()) {
            client.reduce(&group, &outputVec);
            JobContex->jobState.fetch_add(1ULL << 2, std::memory_order_acq_rel); // increment the progress counter
        }
    }
}
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
void emit3(K3* key, V3* value, void* context)
{
    // Cast the context to an OutputVec pointer
    auto outputVec = static_cast<OutputVec*>(context);
    if (!outputVec) {
        std::fprintf(stderr, "system error: invalid output vector context\n");
        std::exit (1);
    }

    // Add the key-value pair to the output vector
    outputVec->emplace_back(key, value);
}

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
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int multiThreadLevel)
    {
      auto jobContext = new (std::nothrow) JobContext(client, inputVec, outputVec, multiThreadLevel);
      if (!jobContext) {
        std::fprintf(stderr, "system error: Failed to allocate memory for "
                             "JobContext\n");
        std::exit(1);
      }
      jobContext->jobState.store(
          (uint64_t)(MAP_STAGE) |
          ((uint64_t)inputVec.size() << 33) | // Set the total number of tasks
          ((uint64_t)0 << 2)); // Set the initial progress to 0

      for (int i = 0; i < multiThreadLevel; ++i) {
        try {
          jobContext->workers.emplace_back([jobContext, i]() {
            // Map phase
            mapPhase(jobContext, i);

            // Sort phase
            sortPhase(jobContext, i);

            // Barrier synchronization to enter shuffle phase
            jobContext->mapSortBarrier.barrier();

            // Only thread 0 performs the shuffle phase
            if (i == 0) {
              unit64_t totalPairs = 0;
              for (int j = 0; j < jobContext->numThreads; ++j) {
                totalPairs += jobContext->intermediates[j].size();
              }
              jobContext->jobState.store(
                  (uint64_t)(SHUFFLE_STAGE) |
                  ((uint64_t)totalPairs << 33) | // Set the total number of tasks
                  ((uint64_t)0 << 2), std::memory_order_release); // Set the
                  // initial progress to 0

              shufflePhase(jobContext);

                jobContext->jobState.store(
                    (uint64_t)(REDUCE_STAGE) |
                    ((uint64_t)totalPairs << 33) | // Set the total number of tasks
                    ((uint64_t)0 << 2), std::memory_order_release); // Set the
                    // initial progress to 0
            }

            // Reduce phase
            reducePhase(jobContext, i);
          });
        } catch (const std::system_error& e) {
          delete jobContext;
          std::fprintf(stderr, "system error: Failed to allocate memory for thread: %s\n",
                       e.what());
          std::exit(1);
        }
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
void waitForJob(JobHandle job)
{
    auto jobContext = static_cast<JobContext*>(job);
    if (!jobContext) {
      std::fprintf(stderr, "system error: invalid job handle\n");
      std::exit (1);

    }

    // Wait for all worker threads to finish
    for (auto& worker : jobContext->workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

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
      std::fprintf(stderr, "system error: invalid job handle\n");
      std::exit (1);    }
    if (!state) {
      std::fprintf(stderr, "system error: invalid job state pointer\n");
      std::exit (1);    }

    // Lock the mutex to safely access the job state
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
void closeJobHandle(JobHandle job)
{
    auto jobContext = static_cast<JobContext*>(job);
    if (!jobContext) {
      std::fprintf(stderr, "system error: invalid job handle\n");
      std::exit (1);    }

    // Clean up the job context
    delete jobContext;
}