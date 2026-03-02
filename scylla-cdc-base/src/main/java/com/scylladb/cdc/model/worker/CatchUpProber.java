package com.scylladb.cdc.model.worker;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.flogger.FluentLogger;
import com.scylladb.cdc.cql.WorkerCQL;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.TaskId;
import com.scylladb.cdc.model.Timestamp;
import com.scylladb.cdc.transport.GroupedTasks;

/**
 * Probes CDC log streams to find the first actual change, allowing the worker
 * to skip empty time windows during catch-up on first startup.
 *
 * <p>For each task without saved state whose window start is older than the
 * catch-up cutoff, this class issues lightweight {@code SELECT ... LIMIT 1}
 * queries against each stream in the task. The earliest result is used to
 * advance the task's window start, avoiding unnecessary reads of empty windows.
 *
 * <p>Concurrency is bounded by a {@link java.util.concurrent.Semaphore} with
 * {@link #MAX_CONCURRENT_PROBES} permits to prevent overwhelming the cluster
 * with probe queries. Each probe acquires a permit before execution and
 * releases it on completion (including cancellation).
 *
 * <p><strong>Timeout semantics:</strong> The {@code probeTimeoutSeconds} applies
 * per candidate task, not per individual stream probe. For each candidate,
 * all stream probes are dispatched concurrently and collected via
 * {@code CompletableFuture.allOf()}. The timeout is applied when waiting
 * for the aggregate result. A candidate with many streams may effectively
 * wait longer than {@code probeTimeoutSeconds} in wall-clock time if probe
 * dispatching itself takes time due to semaphore contention.
 *
 * <p>Instances are short-lived — created per {@code Worker.createTasksWithState()}
 * invocation and not shared across threads. However, the probe futures execute
 * asynchronously on the CQL driver's I/O threads.
 */
final class CatchUpProber {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    static final int MAX_CONCURRENT_PROBES = 64;

    private final WorkerCQL cql;
    private final long queryTimeWindowSizeMs;
    private final long probeTimeoutSeconds;
    private final int maxConcurrentProbes;

    CatchUpProber(WorkerCQL cql, long queryTimeWindowSizeMs, long probeTimeoutSeconds) {
        this(cql, queryTimeWindowSizeMs, probeTimeoutSeconds, MAX_CONCURRENT_PROBES);
    }

    CatchUpProber(WorkerCQL cql, long queryTimeWindowSizeMs, long probeTimeoutSeconds, int maxConcurrentProbes) {
        this.cql = cql;
        this.queryTimeWindowSizeMs = queryTimeWindowSizeMs;
        this.probeTimeoutSeconds = probeTimeoutSeconds;
        this.maxConcurrentProbes = maxConcurrentProbes;
    }

    private static final class ProbeCandidate {
        final Task task;
        final int index;

        ProbeCandidate(Task task, int index) {
            this.task = task;
            this.index = index;
        }
    }

    /**
     * Applies catch-up probes to tasks that have no saved state and whose window start
     * is older than the catch-up cutoff. For each such task, probes the CDC log to find
     * the first actual change and advances the task's window start to that point.
     *
     * @return a new list with probe results applied; entries whose windows are advanced
     *         are replaced, all others are unchanged. Returns the input list as-is if
     *         no probing is needed.
     */
    List<Task> apply(List<Task> tasks, Map<TaskId, TaskState> states,
               GroupedTasks workerTasks, Optional<Date> catchUpCutoffOpt) {
        if (!catchUpCutoffOpt.isPresent()) {
            return tasks;
        }
        if (!workerTasks.getGenerationMetadata().isClosed()) {
            logger.atFine().log("Catch-up probing skipped: generation %s is still open",
                    workerTasks.getGenerationId());
            return tasks;
        }

        Date catchUpCutoff = catchUpCutoffOpt.get();
        List<ProbeCandidate> candidates = collectCandidates(tasks, states, catchUpCutoff);

        if (candidates.isEmpty()) {
            logger.atFine().log("Catch-up enabled but all %d tasks have saved state or recent windows; no probing needed",
                    tasks.size());
            return tasks;
        }

        List<CompletableFuture<Optional<Timestamp>>> probeFutures = dispatchProbes(candidates);
        Map<Integer, Task> replacements = collectAndClampResults(candidates, probeFutures, workerTasks);
        return buildReplacementList(tasks, replacements);
    }

    private List<ProbeCandidate> collectCandidates(List<Task> tasks, Map<TaskId, TaskState> states, Date catchUpCutoff) {
        List<ProbeCandidate> candidates = new ArrayList<>();
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            if (!states.containsKey(task.id)
                    && task.state.getWindowStartTimestamp().toDate().before(catchUpCutoff)) {
                candidates.add(new ProbeCandidate(task, i));
            }
        }
        return candidates;
    }

    private List<CompletableFuture<Optional<Timestamp>>> dispatchProbes(List<ProbeCandidate> candidates) {
        Semaphore probeSemaphore = new Semaphore(maxConcurrentProbes);
        boolean interrupted = false;
        List<CompletableFuture<Optional<Timestamp>>> probeFutures = new ArrayList<>();
        for (ProbeCandidate c : candidates) {
            List<CompletableFuture<Optional<Timestamp>>> streamProbes = new ArrayList<>();
            if (interrupted) {
                CompletableFuture<Optional<Timestamp>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new InterruptedException("Skipped due to earlier interrupt"));
                streamProbes.add(failed);
            } else {
                for (StreamId stream : c.task.streams) {
                    if (interrupted) {
                        break;
                    }
                    try {
                        probeSemaphore.acquire();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        interrupted = true;
                        CompletableFuture<Optional<Timestamp>> failed = new CompletableFuture<>();
                        failed.completeExceptionally(e);
                        streamProbes.add(failed);
                        break;
                    }
                    // whenComplete fires even on cancellation (cancel() triggers
                    // completeExceptionally(CancellationException)), ensuring the
                    // semaphore is always released.
                    streamProbes.add(cql.fetchFirstChangeTime(
                            c.task.id.getTable(), stream, c.task.state.getWindowStartTimestamp(),
                            TimeUnit.SECONDS.toMillis(probeTimeoutSeconds))
                            .whenComplete((r, ex) -> probeSemaphore.release()));
                }
            }
            probeFutures.add(CompletableFuture.allOf(streamProbes.toArray(new CompletableFuture[0]))
                    .thenApply(v -> streamProbes.stream()
                            .map(f -> f.getNow(Optional.empty()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .min(Timestamp::compareTo)));
        }
        return probeFutures;
    }

    private Map<Integer, Task> collectAndClampResults(List<ProbeCandidate> candidates,
            List<CompletableFuture<Optional<Timestamp>>> probeFutures, GroupedTasks workerTasks) {
        Map<Integer, Task> replacements = new HashMap<>();
        int advanced = 0;
        for (int j = 0; j < candidates.size(); j++) {
            ProbeCandidate candidate = candidates.get(j);
            Optional<Timestamp> probeResult = collectSingleResult(candidate, probeFutures, j);
            if (probeResult == null) {
                // Interrupted — cancel remaining and stop.
                cancelRemainingProbes(probeFutures, j + 1);
                break;
            }
            if (probeResult.isPresent()) {
                Timestamp clamped = clampToGeneration(probeResult.get(), workerTasks);
                if (clamped.compareTo(candidate.task.state.getWindowStartTimestamp()) > 0) {
                    TaskState newState = TaskState.createForWindow(clamped, queryTimeWindowSizeMs);
                    replacements.put(candidate.index, new Task(candidate.task.id, candidate.task.streams, newState));
                    logger.atFine().log("Catch-up probe: advancing task %s from %s to %s",
                            candidate.task.id, candidate.task.state.getWindowStartTimestamp(), clamped);
                    advanced++;
                }
            }
        }

        if (!candidates.isEmpty()) {
            logger.atInfo().log("Catch-up probing complete: %d candidates probed, %d tasks advanced",
                    candidates.size(), advanced);
        }
        return replacements;
    }

    /**
     * Collects the probe result for a single candidate. Returns {@code null} if the
     * thread was interrupted (caller should break). Returns {@code Optional.empty()}
     * if the probe timed out, failed, or found no data.
     */
    private Optional<Timestamp> collectSingleResult(ProbeCandidate candidate,
            List<CompletableFuture<Optional<Timestamp>>> probeFutures, int index) {
        try {
            return probeFutures.get(index).get(probeTimeoutSeconds, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            probeFutures.get(index).cancel(true);
            logger.atWarning().log(
                    "Catch-up probe timed out for task %s after %d seconds, falling back to original window start",
                    candidate.task.id, probeTimeoutSeconds);
            return Optional.empty();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                logger.atWarning().log("Catch-up probe interrupted for task %s, aborting remaining probes",
                        candidate.task.id);
                return null;
            }
            logger.atWarning().withCause(cause).log(
                    "Catch-up probe failed for task %s, falling back to original window start",
                    candidate.task.id);
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.atWarning().log("Catch-up probe interrupted for task %s, falling back to original window start",
                    candidate.task.id);
            return null;
        }
    }

    /**
     * Clamps a probe timestamp to [generationStart, generationEnd - 1ms] so the
     * window start stays strictly within this generation's time range.
     */
    private Timestamp clampToGeneration(Timestamp probeTime, GroupedTasks workerTasks) {
        Timestamp generationStart = workerTasks.getGenerationId().getGenerationStart();
        if (probeTime.compareTo(generationStart) < 0) {
            probeTime = generationStart;
        }
        Optional<Timestamp> generationEnd = workerTasks.getGenerationMetadata().getEnd();
        if (generationEnd.isPresent()) {
            Timestamp maxStart = generationEnd.get().plus(-1, ChronoUnit.MILLIS);
            if (probeTime.compareTo(maxStart) > 0) {
                probeTime = maxStart;
            }
        }
        return probeTime;
    }

    private static void cancelRemainingProbes(List<CompletableFuture<Optional<Timestamp>>> probeFutures, int fromIndex) {
        for (int k = fromIndex; k < probeFutures.size(); k++) {
            probeFutures.get(k).cancel(true);
        }
    }

    private List<Task> buildReplacementList(List<Task> tasks, Map<Integer, Task> replacements) {
        if (replacements.isEmpty()) {
            return tasks;
        }
        List<Task> result = new ArrayList<>(tasks.size());
        for (int i = 0; i < tasks.size(); i++) {
            result.add(replacements.getOrDefault(i, tasks.get(i)));
        }
        return result;
    }
}
