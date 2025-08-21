package lblod.info.datasetdiff;

import java.util.ArrayList;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.utils.ModelUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AppService {
    private final TaskService taskService;
    @Value("${application.sleepBetweenJob}")
    private int sleepMs;

    private void sleep(int sleepMs) {
        try {
            log.info("sleep for {} ms to let virtuoso breathe", sleepMs);
            Thread.sleep(sleepMs);
        } catch (Throwable e) {
            log.error("could not sleep", e);
        }
    };

    @Value("${sparql.defaultLimitSize}")
    private int defaultLimitSize;

    public AppService(TaskService taskService) {
        this.taskService = taskService;
    }

    public void runAsync(String deltaEntry) {

        Thread.startVirtualThread(() -> {

            if (!taskService.isTask(deltaEntry))
                return;
            var taskWithJobId = taskService.loadTask(deltaEntry);

            if (taskWithJobId == null || StringUtils.isEmpty(taskWithJobId.task().getOperation())) {
                log.debug("task or operation is empty for delta entry {}", deltaEntry);
                return;
            }
            var task =taskWithJobId.task();

            if (Constants.TASK_HARVESTING_DATASET_DIFF.equals(task.getOperation())) {
                try {

                    taskService.updateTaskStatus(task, Constants.STATUS_BUSY);
                    var inputContainer = taskService.selectInputContainer(task).get(0);
                    log.info("input container: {}", inputContainer);
                    var countTriples = taskService.countTriplesFromFileInputContainer(
                            inputContainer.getGraphUri());
                    var pagesCount = countTriples > defaultLimitSize
                            ? countTriples / defaultLimitSize
                            : 1;

                    var fileContainer = DataContainer.builder().build();

                    var graphContainer = DataContainer.builder().build();
                    var resultContainer = DataContainer.builder().graphUri(graphContainer.getUri()).build();

                    for (var i = 0; i <= pagesCount; i++) {
                        var threads = new ArrayList<Thread>();
                        var offset = i * defaultLimitSize;
                        var importedTriples = taskService.fetchTripleFromFileInputContainer(
                                inputContainer.getGraphUri(), defaultLimitSize, offset);
                        for (var mdb : importedTriples) {
                            threads.add(Thread.startVirtualThread(() -> {
                                var previousCompletedModel = taskService.fetchTripleFromPreviousJobs(task,
                                        mdb.derivedFrom());
                                sleep(sleepMs);
                                var newInserts = ModelUtils.difference(mdb.model(), previousCompletedModel);
                                var toRemoveOld = ModelUtils.difference(previousCompletedModel, mdb.model());
                                var intersection = ModelUtils.intersection(mdb.model(), previousCompletedModel);
                                if (!newInserts.isEmpty()) {
                                    var dataDiffContainer = fileContainer.toBuilder()
                                            .graphUri(taskService.writeTtlFile(
                                                    task.getGraph(), newInserts,
                                                    "new-insert-triples.ttl", mdb.derivedFrom(), taskWithJobId.jobId()))
                                            .build();
                                    taskService.appendTaskResultFile(task, dataDiffContainer);

                                    taskService.appendTaskResultFile(
                                            task, graphContainer.toBuilder()
                                                    .graphUri(dataDiffContainer.getGraphUri())
                                                    .build());
                                }
                                if (!toRemoveOld.isEmpty()) {
                                    var dataRemovalsContainer = fileContainer.toBuilder()
                                            .graphUri(taskService.writeTtlFile(
                                                    task.getGraph(), toRemoveOld,
                                                    "to-remove-triples.ttl", mdb.derivedFrom(), taskWithJobId.jobId()))
                                            .build();
                                    taskService.appendTaskResultFile(task, dataRemovalsContainer);
                                }
                                if (!intersection.isEmpty()) {
                                    var dataIntersectContainer = fileContainer.toBuilder()
                                            .graphUri(taskService.writeTtlFile(
                                                    task.getGraph(), intersection,
                                                    "intersect-triples.ttl", mdb.derivedFrom(), taskWithJobId.jobId()))
                                            .build();
                                    taskService.appendTaskResultFile(task, dataIntersectContainer);
                                }
                            }));
                            sleep(sleepMs);
                        }
                        for (var thread : threads) {
                            thread.join();
                        }
                    }

                    taskService.appendTaskResultGraph(task, resultContainer);
                    taskService.updateTaskStatus(task, Constants.STATUS_SUCCESS);
                    log.info("Done with success for task {}", task.getId());
                } catch (Throwable e) {
                    log.error("Error:", e);
                    taskService.updateTaskStatus(task, Constants.STATUS_FAILED);
                    taskService.appendTaskError(task, e.getMessage());
                }
            } else {
                log.debug("unknown operation '{}' for delta entry {}",
                        task.getOperation(), deltaEntry);
            }
        });
    }
}
