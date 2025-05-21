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
    std::mutex shuffleMutex;
    std::vector<IntermediateVec> shuffleQueue;
    std::atomic<uint64_t> queueSize{0};
    std::atomic<bool> shuffleDone{false};

    // --- Reduce phase start synchronization ---
    std::mutex reducePhaseStartMutex;
    std::condition_variable reducePhaseStartCV;

    // --- Output vector mutex ---
    std::mutex outputMutex;

    // --- Job state & progress counters ---
    std::atomic<uint64_t> jobState{0};

    // --- Barrier for synchronization ---
    Barrier mapSortBarrier;

    JobContext (const MapReduceClient &client_ref, const InputVec &inputVec_ref, OutputVec &outputVec_ref, int numThreads_ref)
        : client (client_ref), inputVec (&inputVec_ref), outputVec (&outputVec_ref), numThreads (numThreads_ref),
          intermediates (numThreads_ref), mapSortBarrier (numThreads_ref) 
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
        if (!currentKey || *candidate < *currentKey)
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
      std::lock_guard<std::mutex> lg (jobContext->shuffleMutex);
      jobContext->shuffleQueue.push_back (std::move (group));
      // 1d) Update the progress counter
      jobContext->jobState.fetch_add (static_cast<uint64_t>(group.size ())
                                          << 2, std::memory_order_acq_rel); // increment the progress counter
      // 1e) Update the queue size

      jobContext->queueSize.fetch_add (1);  // your atomic counter for number of groups
    }
  }


  // 2) Signal “no more groups”
  jobContext->shuffleDone.store (true, std::memory_order_release);
}


void reducePhase (JobContext *ctx)
{
  while (true)
  {
    IntermediateVec group;
    bool popped_group = false;

    if (ctx->queueSize.load(std::memory_order_acquire) == 0) {
      if (ctx->shuffleDone.load(std::memory_order_acquire)) {
        break; 
      }
      std::this_thread::yield(); 
      continue;
    }

    {
      std::lock_guard<std::mutex> lg(ctx->shuffleMutex);
      if (!ctx->shuffleQueue.empty()) {
        group = std::move(ctx->shuffleQueue.back());
        ctx->shuffleQueue.pop_back();
        ctx->queueSize.fetch_sub(1, std::memory_order_acq_rel);
        popped_group = true;
      }
    }

    if (popped_group && !group.empty ())
    {
      ctx->client.reduce (&group, ctx); 
      ctx->jobState.fetch_add (static_cast<uint64_t>(group.size()) << 2, 
                                      std::memory_order_acq_rel);
    }
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

JobHandle startMapReduceJob(
    const MapReduceClient &client_ref,
    const InputVec       &inputVec,
    OutputVec            &outputVec,
    int                    multiThreadLevel)
{
    // 1) Clamp thread count to [1 .. inputVec.size()]
    int numThreads = std::min(multiThreadLevel, (int)inputVec.size());
    if (numThreads < 1) numThreads = 1;

    // 2) Allocate the shared context
    auto *ctx = new (std::nothrow)
        JobContext(client_ref, inputVec, outputVec, numThreads);
    if (!ctx) {
        std::fprintf(stderr, "system error: Failed to allocate JobContext\n");
        std::exit(1);
    }

    // 3) Publish initial MAP_STAGE: total = inputVec.size(), progress = 0
    ctx->jobState.store(
        (uint64_t(inputVec.size()) << 33) |
        (0ULL << 2) |
        uint64_t(MAP_STAGE),
        std::memory_order_release
    );

    // 4) === fast‐path for a single thread ===
    if (numThreads == 1) {
        // -- map all inputs --
        mapPhase(ctx, /*threadID=*/0);

        // mark MAP done
        ctx->jobState.store(
            (uint64_t(inputVec.size()) << 33) |
            (uint64_t(inputVec.size()) << 2) |
            uint64_t(MAP_STAGE),
            std::memory_order_release
        );

        // -- sort the one intermediate vector --
        sortPhase(ctx, /*threadID=*/0);

        // now do grouping+reduce directly on intermediates[0]:
        auto &iv = ctx->intermediates[0];
        // because sortPhase already barrier-synced (and here only one thread),
        // `iv` is sorted ascending by key
        size_t i = 0, n = iv.size();
        while (i < n) {
            // collect one group of identical keys
            K2 *currentKey = iv[i].first;
            IntermediateVec group;
            while (i < n && !(*iv[i].first < *currentKey) && !(*currentKey < *iv[i].first)) {
                group.push_back(iv[i]);
                ++i;
            }
            // call reduce on that group
            ctx->client.reduce(&group, ctx);
        }

        return static_cast<JobHandle>(ctx);
    }

    // 5) === multi‐threaded path ===
    try {
        for (int t = 0; t < numThreads; ++t) {
            ctx->workers.emplace_back([ctx, t]() {
                mapPhase(ctx, t);
                sortPhase(ctx, t);

                // barrier in sortPhase synchronizes all threads
                if (t == 0) {
                    // only thread 0 does shuffle + wake others
                    uint64_t totalInter = 0;
                    for (int k = 0; k < ctx->numThreads; ++k)
                        totalInter += ctx->intermediates[k].size();

                    ctx->jobState.store(
                        (totalInter << 33) |
                        (0ULL << 2) |
                        uint64_t(SHUFFLE_STAGE),
                        std::memory_order_release
                    );

                    shufflePhase(ctx);
                    {
                        std::lock_guard<std::mutex> lk(ctx->reducePhaseStartMutex);
                        ctx->shuffleDone.store(true, std::memory_order_release);
                    }
                    ctx->reducePhaseStartCV.notify_all();

                    uint64_t totForReduce = JobContext::calculateTotal(
                        ctx->jobState.load(std::memory_order_acquire));
                    ctx->jobState.store(
                        (totForReduce << 33) |
                        (0ULL << 2) |
                        uint64_t(REDUCE_STAGE),
                        std::memory_order_release
                    );
                }
                else {
                    std::unique_lock<std::mutex> lk(ctx->reducePhaseStartMutex);
                    ctx->reducePhaseStartCV.wait(lk, [ctx]() {
                        return ctx->shuffleDone.load(std::memory_order_acquire);
                    });
                }

                reducePhase(ctx);
            });
        }
    }
    catch (const std::system_error &e) {
        std::fprintf(stderr, "system error: %s\n", e.what());
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

  uint64_t atomicValue = jobContext->jobState.load(std::memory_order_acquire);
  uint32_t total = JobContext::calculateTotal (atomicValue);
  uint32_t progress = JobContext::calculateProgress (atomicValue);
  state->stage = JobContext::calculateStage (atomicValue);

  if (state->stage == UNDEFINED_STAGE) {
      state->percentage = 0.0f; 
  } else if (total == 0) {
      state->percentage = 100.0f; 
  } else {
    float p_val = static_cast<float>(progress);
    float t_val = static_cast<float>(total);
    if (p_val >= t_val) { 
        state->percentage = 100.0f;
    } else {
        state->percentage = 100.0f * (p_val / t_val);
    }
  }
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