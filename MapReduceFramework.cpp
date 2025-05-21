#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "Barrier.h" // Make sure this is your correct Barrier implementation
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <queue>
#include <algorithm>
#include <cstdio>
#include <system_error>
#include <condition_variable> // For improved synchronization
#include <iostream>
// Define constants or macros if needed
// #define UNDEFINED_PERCENTAGE -1.0f // Not strictly needed now

struct JobContext
{
    // --- Client & data pointers ---
    const MapReduceClient &client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    const int numThreads;

    // --- Thread management ---
    std::vector<std::thread> workers;

    // --- Thread-specific data ---
    std::vector<IntermediateVec> intermediates;

    // --- Input-claiming counter (map) ---
    std::atomic<size_t> nextInputIndex{0};

    // --- Shuffle-phase queue & coordination ---
//    std::mutex shuffleMutex;
    std::vector<IntermediateVec> shuffleVec;
//    std::queue<IntermediateVec> shuffleVec;
    std::atomic<uint64_t> queueSize{0};
//    std::atomic<bool> shuffleDone{false};

    // --- Reduce phase start synchronization ---
//    std::condition_variable reducePhaseStartCV;

    // --- Output vector mutex ---
    std::mutex outputMutex;

    // --- Job state & progress counters ---
    std::atomic<uint64_t> jobState{0};

    // --- Barrier for synchronization ---
    Barrier mapSortBarrier;
    std::atomic<uint64_t> atomic_reduce;

    JobContext (const MapReduceClient &client_ref, const InputVec &inputVec_ref, OutputVec &outputVec_ref, int numThreads_ref)
        : client (client_ref), inputVec (&inputVec_ref), outputVec (&outputVec_ref), numThreads (numThreads_ref),
          intermediates (numThreads_ref), mapSortBarrier (numThreads_ref), atomic_reduce(0)
    {
    }

    // Static function to calculate the total number of tasks (bits 33–63, 31 bits)
    static uint32_t calculateTotal (uint64_t currentJobState)
    {
      return static_cast<uint32_t>((currentJobState >> 33) & 0x7FFFFFFF); 
    }

    // Static function to calculate the number of completed tasks (bits 2–32, 31 bits)
    static uint32_t calculateProgress (uint64_t currentJobState)
    {
      return static_cast<uint32_t>((currentJobState >> 2) & 0x7FFFFFFF); 
    }
    // Static function to calculate the current stage (bits 0–1)
    static stage_t calculateStage (uint64_t currentJobState)
    {
      return static_cast<stage_t>(currentJobState & 0x3); 
    }
};

auto keysEqual = [] (const K2 *a, const K2 *b)
{
    if (a == nullptr || b == nullptr) return a == b; 
    return !(*a < *b) && !(*b < *a);
};

// Single, corrected definition of mapPhase
void mapPhase (JobContext *ctx, int threadID)
{
  const auto &inputVector = *ctx->inputVec;
  // The context for emit2 is the address of this thread's specific IntermediateVec.
  // This is passed to client.map.

  while(true)
  {
    size_t inputIndex = ctx->nextInputIndex.fetch_add (1, std::memory_order_relaxed);
    if (inputIndex >= inputVector.size ())
    {
      break; 
    }
    // Pass the address of the current thread's intermediate vector as context to map
    ctx->client.map(inputVector[inputIndex].first, inputVector[inputIndex].second, &ctx->intermediates[threadID]);
    ctx->jobState.fetch_add (1ULL << 2, std::memory_order_acq_rel);
  }
}


void emit2 (K2 *key, V2 *value, void *context)
{
  auto intermediateVec = static_cast<IntermediateVec *>(context);
  if (!intermediateVec)
  {
    std::fprintf (stdout, "system error: invalid emit2 context\n");
    std::fflush(stdout);
    std::exit (1);
  }
  intermediateVec->emplace_back (key, value);
}

void sortPhase (JobContext *ctx, int threadID)
{
  auto &intermediateVec = ctx->intermediates[threadID];
  std::sort (intermediateVec.begin (), intermediateVec.end (),
             [] (const IntermediatePair &a, const IntermediatePair &b)
             {
                 return *a.first < *b.first;
             });
  ctx->mapSortBarrier.barrier ();
}

void shufflePhase (JobContext *jobContext)
{
  // 1) Keep going until all per-thread vectors are drained
  while (true)
  {
    // 1a) Find the next key to process:
    K2 *currentKey = nullptr;
    for (int t = 0; t < jobContext->numThreads; ++t)
    {
      auto &vec = jobContext->intermediates[t];
      if (!vec.empty ())
      {
        K2 *candidate = vec.back ().first;
        if (!currentKey || *currentKey < *candidate)
        {
          currentKey = candidate;
        }
      }
    }
    if (!currentKey) break;  // done all keys

    // 1b) Peel off _all_ pairs == currentKey from each thread’s vector
    IntermediateVec group;
    for (int t = 0; t < jobContext->numThreads; ++t)
    {
      auto &vec = jobContext->intermediates[t];
      while (!vec.empty () && keysEqual (vec.back ().first, currentKey))
      {
        group.push_back (vec.back ());
        vec.pop_back ();
      }
    }

    // 1c) Push that group onto the shared queue
    {
//      std::lock_guard<std::mutex> lg (jobContext->shuffleMutex);
      jobContext->shuffleVec.emplace_back(std::move (group));
      // 1d) Update the progress counter
      jobContext->jobState.fetch_add (1ULL<<2,std::memory_order_acq_rel); // increment the progress counter
      // 1e) Update the queue size

      jobContext->queueSize.fetch_add (1);  // your atomic counter for number of groups
    }
  }
  uint64_t totalTasks = jobContext->queueSize.load(std::memory_order_acquire);
  uint64_t currentState = jobContext->jobState.load(std::memory_order_acquire);
  uint64_t newState = (currentState & ~(0x7FFFFFFFULL << 33)) | (totalTasks << 33);
  jobContext->jobState.store(newState, std::memory_order_release);

  // 2) Signal “no more groups”
//  jobContext->shuffleDone.store (true, std::memory_order_release);
}





void reducePhase (JobContext *ctx)
{
  while (true)
  {
      size_t inputIndex = ctx->atomic_reduce.fetch_add (1);
      if (inputIndex >= ctx->queueSize)
      {
          break;
      }
      // Pass the address of the current thread's intermediate vector as context to map
      ctx->client.reduce(&ctx->shuffleVec[inputIndex], ctx);
      ctx->jobState.fetch_add (1ULL << 2, std::memory_order_acq_rel);
//    IntermediateVec group;
//    bool popped_group = false;
//
//    if (ctx->queueSize.load(std::memory_order_acquire) == 0) {
//      if (ctx->shuffleDone.load(std::memory_order_acquire)) {
//        break;
//      }
//      std::this_thread::yield();
//      continue;
//    }
//
//    {
////      std::lock_guard<std::mutex> lg(ctx->shuffleMutex);
//      if (!ctx->shuffleVec.empty()) {
//        group = std::move(ctx->shuffleVec.front());
//        ctx->shuffleVec.pop();
//        ctx->queueSize.fetch_sub(1, std::memory_order_acq_rel);
//        popped_group = true;
//      }
//    }
//
//    if (popped_group && !group.empty ())
//    {
//      ctx->client.reduce (&group, ctx);
//      ctx->jobState.fetch_add (1ULL << 2,
//                                      std::memory_order_acq_rel);
//    }
  }
}

void emit3 (K3 *key, V3 *value, void *context)
{
  auto jobCtx = static_cast<JobContext *>(context); 
  if (!jobCtx)
  {
    std::fprintf (stdout, "system error: invalid emit3 context\n");
    std::fflush(stdout);
    std::exit (1);
  }
  std::lock_guard<std::mutex> lg (jobCtx->outputMutex); 
  jobCtx->outputVec->emplace_back (key, value);
}

JobHandle startMapReduceJob(const MapReduceClient &client_ref, 
                           const InputVec   &inputVec,
                           OutputVec        &outputVec,
                           int               multiThreadLevel)
{
    auto ctx = new (std::nothrow) JobContext(client_ref, inputVec, outputVec, multiThreadLevel);
    if (!ctx) {
        std::fprintf(stdout, "system error: Failed to allocate memory for JobContext\n");
        std::fflush(stdout);
        std::exit(1);
    }

    ctx->jobState.store(
        ((uint64_t)inputVec.size() << 33) | 
        (0ULL << 2) |                       
        (uint64_t)MAP_STAGE,                
        std::memory_order_release
    );

    try {
        for (int i = 0; i < multiThreadLevel; ++i) {
            ctx->workers.emplace_back([ctx, i]() { 
                mapPhase(ctx, i); 
                sortPhase(ctx, i);
                
                if (i == 0) {
                    uint64_t state_after_shuffle = ctx->jobState.load(std::memory_order_acquire);
                    uint32_t total_items_for_reduce = JobContext::calculateTotal(state_after_shuffle);
                    ctx->jobState.store(
                            ((uint64_t)total_items_for_reduce << 33) |
                            (0ULL << 2) |
                            (uint64_t)SHUFFLE_STAGE ,
                            std::memory_order_release
                    );
//                    std::cout<<"blaa    "<<JobContext::calculateStage(ctx->jobState.load());
                    shufflePhase(ctx); 

                    state_after_shuffle = ctx->jobState.load(std::memory_order_acquire);
                     total_items_for_reduce = JobContext::calculateTotal(state_after_shuffle);

                    ctx->jobState.store(
                        ((uint64_t)total_items_for_reduce << 33) |
                        (0ULL << 2) |
                        (uint64_t)REDUCE_STAGE ,
                        std::memory_order_release
                    );
                    
//                    ctx->reducePhaseStartCV.notify_all();
                }
                ctx->mapSortBarrier.barrier();

//                else {
//                    std::unique_lock<std::mutex> lk(ctx->reducePhaseStartMutex);
//                    ctx->reducePhaseStartCV.wait(lk, [ctx]() {
//                        return ctx->shuffleDone.load(std::memory_order_acquire);
//                    });
//                }
                reducePhase(ctx);
                ctx->mapSortBarrier.barrier();
            });
        }
    }
    catch (const std::system_error &e) { 
        std::fprintf(stdout, "system error: %s\n", e.what());
        std::fflush(stdout);
        std::exit(1);
    }
    catch (const std::exception &e) { 
        std::fprintf(stdout, "system error: %s\n", e.what());
        std::fflush(stdout);
        std::exit(1);
    }
    return static_cast<JobHandle>(ctx);
}

void waitForJob (JobHandle job)
{
  auto jobContext = static_cast<JobContext *>(job);
  if (!jobContext)
  {
    return; 
  }
  for (auto &worker: jobContext->workers)
  {
    if (worker.joinable ())
    {
      worker.join ();
    }
  }
}

void getJobState (JobHandle job, JobState *state)
{
  auto jobContext = static_cast<JobContext *>(job);
  if (!jobContext) {
       std::fprintf(stdout, "system error: invalid job handle\n");
       std::fflush(stdout);
       std::exit(1);
  }
  if (!state) {
       std::fprintf(stdout, "system error: invalid job state pointer\n");
       std::fflush(stdout);
       std::exit(1);
  }
  JobContext* ctx = static_cast<JobContext*>(job);
  uint64_t atomicValue = ctx->jobState.load
      (std::memory_order_acquire);
  uint32_t total = JobContext::calculateTotal (atomicValue);
  uint32_t progress = JobContext::calculateProgress (atomicValue);
  state->stage = JobContext::calculateStage (atomicValue);
    float p_val = static_cast<float>(progress);
    float t_val = static_cast<float>(total);
//    std::cout<<p_val<<"   total:"<<t_val<<std::endl;
    if (p_val >= t_val) {
        state->percentage = 100.0f;
    }
    else {
        state->percentage = 100.0f * (p_val / t_val);
    }
//    }
//  if (state->stage == UNDEFINED_STAGE) {
//      state->percentage = 0.0f;
//  } else if (total == 0) {
//      state->percentage = 100.0f;
//  } else {
//    float p_val = static_cast<float>(progress);
//    float t_val = static_cast<float>(total);
//    if (p_val >= t_val) {
//        state->percentage = 100.0f;
//    } else {
//        state->percentage = 100.0f * (p_val / t_val);
//    }
//  }
}

void closeJobHandle (JobHandle job)
{
  auto jobContext = static_cast<JobContext *>(job);
  if (!jobContext) {
      std::fprintf(stdout, "system error: invalid job handle\n");
      std::fflush(stdout);
      std::exit(1);
  }
  waitForJob (job); 
  delete jobContext;
}

uint32_t getTotalTasks(JobHandle job) {
  auto jobContext = static_cast<JobContext*>(job);
  if (!jobContext) {
    std::fprintf(stdout, "system error: invalid job handle\n");
    std::fflush(stdout);
    std::exit(1);
  }
  uint64_t atomicValue = jobContext->jobState.load(std::memory_order_acquire);
  return JobContext::calculateTotal(atomicValue);
}
