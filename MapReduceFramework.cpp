#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier/Barrier.h"
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
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
    std::atomic<bool> shuffleDone{false};

    // — Job state & progress counters —
    std::atomic<uint64_t> jobState{0};

    // — Barrier for synchronization —
    Barrier mapSortBarrier;

    JobContext(const MapReduceClient& client, const InputVec& inputVec, OutputVec& outputVec, int numThreads)
        : client(client), inputVec(&inputVec), outputVec(&outputVec), numThreads(numThreads),
          intermediates(numThreads), mapSortBarrier(numThreads) {
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
    }
}

void emit2(K2* key, V2* value, void* context)
{
  // Cast the context to an IntermediateVec pointer
  auto intermediateVec = static_cast<IntermediateVec*>(context);
  if (!intermediateVec) {
    std::cerr << "Invalid intermediate vector context" << std::endl;
    return;
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

void shufflePhase(JobContext* jobContext) {
    // Only thread 0 performs the shuffle phase
    auto& shuffleQueue = jobContext->shuffleQueue;
    auto& intermediateVec = jobContext->intermediates;

    // Move all intermediate vectors to the shuffle queue
    for (int i = 0; i < jobContext->numThreads; ++i) {
        shuffleQueue.push(std::move(intermediateVec[i]));
    }

    // Mark shuffle as done
    jobContext->shuffleDone.store(true);
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
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec&
inputVec,OutputVec& outputVec, int multiThreadLevel)
{
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
            jobContext->mapSortBarrier.barrier();

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
    }  } catch (const std::system_error& e) {
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