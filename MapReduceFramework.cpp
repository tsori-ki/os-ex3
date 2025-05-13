#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>

// Define constants or macros if needed
#define UNDEFINED_PERCENTAGE -1.0f


typedef struct {
    JobState state;


} JobContext;

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
 * @param value Pointer to the value of type V3. The value must remain valid until the end of the job.
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
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel);

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
void getJobState(JobHandle job, JobState* state);

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