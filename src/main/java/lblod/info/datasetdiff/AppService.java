package lblod.info.datasetdiff;

import org.apache.commons.lang3.StringUtils;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.utils.ModelUtils;

@Slf4j
@Service
public class AppService {
    private final TaskService taskService;

    public AppService(TaskService taskService) {
        this.taskService = taskService;
    }

    @Async
    public void runAsync(String deltaEntry) {

        if (!taskService.isTask(deltaEntry))
            return;
        var task = taskService.loadTask(deltaEntry);

        if (task == null || StringUtils.isEmpty(task.getOperation())) {
            log.debug("task or operation is empty for delta entry {}", deltaEntry);
            return;
        }

        if (Constants.TASK_HARVESTING_DATASET_DIFF.equals(task.getOperation())) {
            try {
                taskService.updateTaskStatus(task, Constants.STATUS_BUSY);
                var inputContainer = taskService.selectInputContainer(task).get(0);
                log.debug("input container: {}", inputContainer);
                var importedTriples = taskService.fetchTripleFromFileInputContainer(inputContainer.getGraphUri());
                var fileContainer = DataContainer.builder().build();

                var previousCompletedModel = taskService.fetchTripleFromPreviousJobs(task);

                var diff = ModelUtils.difference(importedTriples, previousCompletedModel);
                var intersection = ModelUtils.intersection(importedTriples, previousCompletedModel);

                var dataDiffContainer = fileContainer.toBuilder()
                        .graphUri(taskService.writeTtlFile(task.getGraph(), diff, ("diff-triples.ttl")))
                        .build();
                taskService.appendTaskResultFile(task, dataDiffContainer);

                var dataIntersectContainer = fileContainer
                        .toBuilder()
                        .graphUri(taskService.writeTtlFile(task.getGraph(), intersection, "intersect-triples.ttl"))
                        .build();
                taskService.appendTaskResultFile(task, dataIntersectContainer);

                var dataContainer = DataContainer.builder()
                        .graphUri(dataDiffContainer.getGraphUri())
                        .build();
                taskService.appendTaskResultFile(task, dataContainer);
                var graphContainer = DataContainer.builder()
                        .graphUri(dataContainer.getUri())
                        .build();
                taskService.appendTaskResultGraph(task, graphContainer);
                taskService.updateTaskStatus(task, Constants.STATUS_SUCCESS);
                log.debug("Done with success for task {}", task.getId());
            } catch (Throwable e) {
                log.error("Error:", e);
                taskService.updateTaskStatus(task, Constants.STATUS_FAILED);
                taskService.appendTaskError(task, e.getMessage());
            }
        } else {
            log.debug("unknown operation '{}' for delta entry {}", task.getOperation(), deltaEntry);
        }

    }
}
