package lblod.info.datasetdiff;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.dto.Task;
import mu.semte.ch.lib.utils.ModelUtils;
import mu.semte.ch.lib.utils.SparqlClient;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Optional.ofNullable;
import static mu.semte.ch.lib.utils.ModelUtils.*;

@Service
@Slf4j
public class TaskService {

    private final SparqlQueryStore queryStore;
    private final SparqlClient sparqlClient;
    @Value("${share-folder.path}")
    private String shareFolderPath;
    @Value("${sparql.defaultBatchSize}")
    private int defaultBatchSize;
    @Value("${sparql.defaultLimitSize}")
    private int defaultLimitSize;
    @Value("${sparql.maxRetry}")
    private int maxRetry;

    public TaskService(SparqlQueryStore queryStore, SparqlClient sparqlClient) {
        this.queryStore = queryStore;
        this.sparqlClient = sparqlClient;
    }

    public boolean isTask(String subject) {
        String queryStr = queryStore.getQuery("isTask").formatted(subject);

        return sparqlClient.executeAskQuery(queryStr);
    }

    public Task loadTask(String deltaEntry) {
        String queryTask = queryStore.getQuery("loadTask").formatted(deltaEntry);

        return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                return null;
            }
            var t = resultSet.next();
            Task task = Task.builder().task(t.getResource("task").getURI())
                    .job(t.getResource("job").getURI())
                    .error(ofNullable(t.getResource("error")).map(Resource::getURI).orElse(null))
                    .id(t.getLiteral("id").getString())
                    .created(t.getLiteral("created").getString())
                    .modified(t.getLiteral("modified").getString())
                    .operation(t.getResource("operation").getURI())
                    .index(t.getLiteral("index").getString())
                    .graph(t.getResource("graph").getURI())
                    .status(t.getResource("status").getURI())
                    .build();
            log.debug("task: {}", task);
            return task;
        });

    }

    @SneakyThrows
    public Model fetchTripleFromPreviousJobs(Task task) {
        var queryForTargetUrl = queryStore.getQueryWithParameters("getTargetUrl", Map.of("job", task.getJob()));
        var targetUrl = sparqlClient.executeSelectQuery(queryForTargetUrl, rs -> {
            if (!rs.hasNext()) {
                return null;
            }
            var qs = rs.next();
            if (qs.getResource("targetUrl") == null)
                return null;
            return qs.getResource("targetUrl").getURI();

        });

        if (StringUtils.isEmpty(targetUrl)) {
            log.error("This service is experimental. We assume there's always a target url to the job");
            log.error("if we cannot find a target url, simply return an empty model");
            return ModelFactory.createDefaultModel();
        }

        var queryForPreviousMirrorededPath = queryStore.getQueryWithParameters("getPreviousInputContainer",
                Map.of("targetUrl", targetUrl));

        var previousMirroredFilePath = sparqlClient.executeSelectQueryAsListMap(queryForPreviousMirrorededPath);

        var models = previousMirroredFilePath.stream()
                .flatMap(map -> map.values().stream())
                .peek(path -> log.info("importing {}", path))
                .map(this::fetchTripleFromFilePath)
                .peek(m -> log.info("done importing. Triples count: {}", m.size()))
                .filter(m -> !m.isEmpty()).toList();
        log.info("done collecting previous mirrored files. Model count: {}. aggrating...", models.size());
        var aggregateModel = ModelFactory.createDefaultModel();
        for (var model : models) {
            log.info("aggregating model with length {}...", model.size());
            aggregateModel.add(model);
            log.info("done aggregating model. current aggregate length: {}", aggregateModel.size());
        }

        log.info("triple from previous jobs extracted. size: {}", aggregateModel.size());

        return aggregateModel;
        // .reduce(ModelFactory.createDefaultModel(), ModelUtils::merge);
    }

    @SneakyThrows
    public Model fetchTripleFromFileInputContainer(String fileContainerUri) {
        var query = queryStore.getQuery("fetchTripleFromFileInputContainer").formatted(fileContainerUri);
        var path = sparqlClient.executeSelectQuery(query, resultSet -> {
            if (!resultSet.hasNext()) {
                return null;
            }
            var qs = resultSet.next();
            if (qs.getResource("path") == null)
                return null;
            return qs.getResource("path").getURI();
        });

        return fetchTripleFromFilePath(path);
    }

    @SneakyThrows
    public Model fetchTripleFromFilePath(String path) {

        var file = ofNullable(path).map(p -> p.replace("share://", ""))
                .filter(StringUtils::isNotEmpty)
                .map(p -> new File(shareFolderPath, p))
                .filter(File::exists);

        if (file.isPresent() && file.get().isFile() && file.get().length() > 0) {
            log.info("found {} file", path);
            try {
                return ModelUtils.toModel(FileUtils.openInputStream(file.get()), Lang.TURTLE);
            } catch (Exception e) {
                log.error("could not extract triples to a model", e);
                return ModelFactory.createDefaultModel();
            }
        } else {
            log.error(" file '{}' not found", path);
            return ModelFactory.createDefaultModel();
        }

    }

    public void updateTaskStatus(Task task, String status) {
        log.info("set task status to {}...", status);

        String queryUpdate = queryStore.getQuery("updateTaskStatus")
                .formatted(status, formattedDate(LocalDateTime.now()), task.getTask());
        sparqlClient.executeUpdateQuery(queryUpdate);
    }

    public void importTriples(Task task, String graph,
            Model model) {
        log.info("running import triples with batch size {}, model size: {}, graph: <{}>", defaultBatchSize,
                model.size(), graph);
        List<Triple> triples = model.getGraph().find().toList(); // duplicate so we can splice
        Lists.partition(triples, defaultBatchSize)
                .stream()
                .parallel()
                .map(batch -> {
                    Model batchModel = ModelFactory.createDefaultModel();
                    Graph batchGraph = batchModel.getGraph();
                    batch.forEach(batchGraph::add);
                    return batchModel;
                })
                .forEach(batchModel -> this.insertModelOrRetry(task, graph, batchModel));
    }

    private void insertModelOrRetry(Task task, String graph, Model batchModel) {
        int retryCount = 0;
        boolean success = false;
        do {
            try {
                sparqlClient.insertModel(graph, batchModel);
                success = true;
                break;
            } catch (Exception e) {
                log.error("an error occurred, retry count {}, max retry {}, error: {}", retryCount, maxRetry, e);
                retryCount += 1;
            }
        } while (retryCount < maxRetry);
        if (!success) {
            this.appendTaskError(task, "Reaching max retries. Check the logs for further details.");
            this.updateTaskStatus(task, Constants.STATUS_FAILED);
        }
    }

    @SneakyThrows
    public String writeTtlFile(String graph,
            Model content,
            String logicalFileName) {
        var rdfLang = filenameToLang(logicalFileName);
        var fileExtension = getExtension(rdfLang);
        var contentType = getContentType(rdfLang);
        var phyId = uuid();
        var phyFilename = "%s.%s".formatted(phyId, fileExtension);
        var path = "%s/%s".formatted(shareFolderPath, phyFilename);
        var physicalFile = "share://%s".formatted(phyFilename);
        var loId = uuid();
        var logicalFile = "%s/%s".formatted(Constants.LOGICAL_FILE_PREFIX, loId);
        var now = formattedDate(LocalDateTime.now());
        var file = ModelUtils.toFile(content, RDFLanguages.NT, path);
        var fileSize = file.length();
        var queryParameters = ImmutableMap.<String, Object>builder()
                .put("graph", graph)
                .put("physicalFile", physicalFile)
                .put("logicalFile", logicalFile)
                .put("phyId", phyId)
                .put("phyFilename", phyFilename)
                .put("now", now)
                .put("fileSize", fileSize)
                .put("loId", loId)
                .put("logicalFileName", logicalFileName)
                .put("fileExtension", "ttl")
                .put("contentType", contentType).build();

        var queryStr = queryStore.getQueryWithParameters("writeTtlFile", queryParameters);
        sparqlClient.executeUpdateQuery(queryStr);
        return logicalFile;
    }

    public void appendTaskResultFile(Task task,
            DataContainer dataContainer) {
        var containerUri = dataContainer.getUri();
        var containerId = dataContainer.getId();
        var fileUri = dataContainer.getGraphUri();
        var queryParameters = Map.of(
                "containerUri", containerUri,
                "containerId", containerId,
                "fileUri", fileUri, "task", task);
        var queryStr = queryStore.getQueryWithParameters("appendTaskResultFile", queryParameters);

        sparqlClient.executeUpdateQuery(queryStr);
    }

    public void appendTaskResultGraph(Task task,
            DataContainer dataContainer) {
        var queryParameters = Map.of(
                "task", task,
                "dataContainer", dataContainer);
        var queryStr = queryStore.getQueryWithParameters("appendTaskResultGraph", queryParameters);
        log.debug(queryStr);
        sparqlClient.executeUpdateQuery(queryStr);

    }

    public List<DataContainer> selectInputContainer(Task task) {
        String queryTask = queryStore.getQuery("selectInputContainerGraph").formatted(task.getTask());

        return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                throw new RuntimeException("Input container graph not found");
            }
            List<DataContainer> graphUris = new ArrayList<>();
            resultSet.forEachRemaining(r -> graphUris.add(DataContainer.builder()
                    .graphUri(r.getResource("graph").getURI())
                    .build()));
            return graphUris;
        });
    }

    public void appendTaskError(Task task, String message) {
        var id = uuid();
        var uri = Constants.ERROR_URI_PREFIX + id;

        Map<String, Object> parameters = Map.of("task", task, "id", id, "uri", uri, "message",
                ofNullable(message).orElse("Unexpected error"));
        var queryStr = queryStore.getQueryWithParameters("appendTaskError", parameters);

        sparqlClient.executeUpdateQuery(queryStr);
    }

}
