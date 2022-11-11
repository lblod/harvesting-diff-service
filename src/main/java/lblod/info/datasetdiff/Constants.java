package lblod.info.datasetdiff;

public interface Constants {
    String TASK_HARVESTING_DATASET_DIFF = "http://lblod.data.gift/id/jobs/concept/TaskOperation/diff";
  
    String SUBJECT_STATUS = "http://www.w3.org/ns/adms#status";
    String STATUS_BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy";
    String STATUS_SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled";
    String STATUS_SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success";
    String STATUS_FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed";
  
    String LOGICAL_FILE_PREFIX = "http://data.lblod.info/id/files";
    String FILTER_GRAPH_PREFIX = "http://mu.semte.ch/graphs/harvesting/tasks/filtering";
    String VALIDATING_GRAPH_PREFIX = "http://mu.semte.ch/graphs/harvesting/tasks/validating";
    String ERROR_URI_PREFIX = "http://redpencil.data.gift/id/jobs/error/";
  }
