package lblod.info.datasetdiff;

import static java.util.Optional.ofNullable;
import static mu.semte.ch.lib.utils.ModelUtils.*;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.DataContainer;
import mu.semte.ch.lib.dto.Task;
import mu.semte.ch.lib.utils.ModelUtils;
import mu.semte.ch.lib.utils.SparqlClient;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class TaskService {
    public record TaskWithJobId(Task task, String jobId) {}
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
    @Value("${sparql.highLoadSparqlEndpoint}")
    private String highLoadSparqlEndpoint;
    @Value("${sparql.endpoint}")
    private String defaultSparqlEndpoint;

    public TaskService(SparqlQueryStore queryStore, SparqlClient sparqlClient) {
        this.queryStore = queryStore;
        this.sparqlClient = sparqlClient;
    }

    public boolean isTask(String subject) {
        String queryStr = queryStore.getQuery("isTask").formatted(subject);

        return sparqlClient.executeAskQuery(queryStr, highLoadSparqlEndpoint, true);
    }

    public TaskWithJobId loadTask(String deltaEntry) {
        String queryTask = queryStore.getQuery("loadTask").formatted(deltaEntry);

        return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                return new TaskWithJobId(null, null);
            }
            var t = resultSet.next();
            var task = new TaskWithJobId(Task.builder()
                    .task(t.getResource("task").getURI())
                    .job(t.getResource("job").getURI())
                    .error(ofNullable(t.getResource("error"))
                            .map(Resource::getURI)
                            .orElse(null))
                    .id(t.getLiteral("id").getString())
                    .created(t.getLiteral("created").getString())
                    .modified(t.getLiteral("modified").getString())
                    .operation(t.getResource("operation").getURI())
                    .index(t.getLiteral("index").getString())
                    .graph(t.getResource("graph").getURI())
                    .status(t.getResource("status").getURI())
                    .build(),t.getLiteral("jobId").getString());
            log.debug("task: {}", task);
            return task;
        }, highLoadSparqlEndpoint, true);
    }

    @SneakyThrows
    public Model fetchTripleFromPreviousJobs(Task task, String derivedFrom) {

        log.info("fetch triple from previous jobs with derived from {}",
                derivedFrom);
        var queryForPreviousMirrorededPath = queryStore.getQueryWithParameters(
                "getPreviousInputContainer", Map.of("targetUrl", derivedFrom));

        var previousMirroredFilePath = sparqlClient.executeSelectQueryAsListMap(
                queryForPreviousMirrorededPath, highLoadSparqlEndpoint, true);

        var models = previousMirroredFilePath.stream()
                .flatMap(map -> map.values().stream())
                .peek(path -> log.info("importing {}", path))
                .map(this::fetchTripleFromFilePath)
                .peek(m -> log.info("done importing. Triples count: {}", m.size()))
                .filter(m -> !m.isEmpty())
                .toList();
        log.info(
                "done collecting previous mirrored files. Model count: {}. aggregating...",
                models.size());
        var aggregateModel = ModelFactory.createDefaultModel();
        for (var model : models) {
            log.info("aggregating model with length {}...", model.size());
            aggregateModel.add(model);
            log.info("done aggregating model. current aggregate length: {}",
                    aggregateModel.size());
        }

        log.info("triple from previous jobs extracted. size: {}",
                aggregateModel.size());

        return aggregateModel;
    }

    record PathByDerived(String derivedFrom, String path) {
    }

    @SneakyThrows
    public int countTriplesFromFileInputContainer(String fileContainerUri) {
        var query = queryStore.getQueryWithParameters(
                "countTripleFromFileInputContainer",
                Map.of("container", fileContainerUri));
        return sparqlClient.executeSelectQuery(query, resultSet -> {
            if (!resultSet.hasNext()) {
                return 0;
            }
            return resultSet.next().getLiteral("count").getInt();
        }, highLoadSparqlEndpoint, true);
    }

    @SneakyThrows
    public List<ModelByDerived> fetchTripleFromFileInputContainer(String fileContainerUri, int limitSize,
            int offset) {
        var query = queryStore.getQueryWithParameters(
                "fetchTripleFromFileInputContainer",
                Map.of("container", fileContainerUri, "limitSize", limitSize,
                        "offsetNumber", offset));
        var pathsByDerived = sparqlClient.executeSelectQuery(query, resultSet -> {
            var byDerived = new ArrayList<PathByDerived>();

            while (resultSet.hasNext()) {
                var qs = resultSet.next();
                byDerived.add(new PathByDerived(qs.getResource("derivedFrom").getURI(),
                        qs.getResource("path").getURI()));
            }

            return byDerived;
        }, highLoadSparqlEndpoint, true);

        if (pathsByDerived.isEmpty()) {
            log.warn(" files '{}' not found", fileContainerUri);
        }

        var modelsByDerived = new ArrayList<ModelByDerived>();
        for (var pbd : pathsByDerived) {
            var path = pbd.path.replace("share://", "");
            var file = new File(shareFolderPath, path);
            if (!file.exists()) {
                throw new RuntimeException("file %s doesn't exist".formatted(path));
            }
            var modelByDerived = new ModelByDerived(
                    pbd.derivedFrom,
                    ModelUtils.toModel(FileUtils.openInputStream(file), Lang.TURTLE));
            modelsByDerived.add(modelByDerived);
        }
        return modelsByDerived;
    }

    @SneakyThrows
    public Model fetchTripleFromFilePath(String path) {

        var file = ofNullable(path)
                .map(p -> p.replace("share://", ""))
                .filter(StringUtils::isNotEmpty)
                .map(p -> new File(shareFolderPath, p))
                .filter(File::exists);

        if (file.isPresent() && file.get().isFile() && file.get().length() > 0) {
            log.info("found {} file", path);
            try {
                var filePresent = file.get();
                InputStream is = FileUtils.openInputStream(filePresent);
                if (filePresent.getName().endsWith("gz")) {
                    is = new GZIPInputStream(is);
                }
                return ModelUtils.toModel(is, Lang.TURTLE);
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
                .formatted(status, formattedDate(LocalDateTime.now()),
                        task.getTask());
        sparqlClient.executeUpdateQuery(queryUpdate, defaultSparqlEndpoint, true);
    }

    @SneakyThrows
    public String writeTtlFile(String graph, Model content,
            String logicalFileName, String derivedFrom, String folderId) {
        var rdfLang = filenameToLang(logicalFileName);
        var fileExtension = getExtension(rdfLang);
        var contentType = getContentType(rdfLang);
        var baseFolder = "%s/%s/diff".formatted(shareFolderPath, folderId);
        var rootDir = new File(baseFolder);
        rootDir.mkdirs();
        var phyId = uuid();
        var phyFilename = "%s.%s".formatted(baseFolder, fileExtension);
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
                .put("derivedFrom", derivedFrom)
                .put("fileSize", fileSize)
                .put("loId", loId)
                .put("logicalFileName", logicalFileName)
                .put("fileExtension", "ttl")
                .put("contentType", contentType)
                .build();

        var queryStr = queryStore.getQueryWithParameters("writeTtlFile", queryParameters);
        sparqlClient.executeUpdateQuery(queryStr, highLoadSparqlEndpoint, true);
        return logicalFile;
    }

    public void appendTaskResultFile(Task task, DataContainer dataContainer) {
        var containerUri = dataContainer.getUri();
        var containerId = dataContainer.getId();
        var fileUri = dataContainer.getGraphUri();
        var queryParameters = Map.of("containerUri", containerUri, "containerId",
                containerId, "fileUri", fileUri, "task", task);
        var queryStr = queryStore.getQueryWithParameters("appendTaskResultFile",
                queryParameters);

        sparqlClient.executeUpdateQuery(queryStr, highLoadSparqlEndpoint, true);
    }

    public void appendTaskResultGraph(Task task, DataContainer dataContainer) {
        var queryParameters = Map.of("task", task, "dataContainer", dataContainer);
        var queryStr = queryStore.getQueryWithParameters("appendTaskResultGraph",
                queryParameters);
        log.debug(queryStr);
        sparqlClient.executeUpdateQuery(queryStr, highLoadSparqlEndpoint, true);
    }

    public List<DataContainer> selectInputContainer(Task task) {
        String queryTask = queryStore.getQuery("selectInputContainerGraph")
                .formatted(task.getTask());

        return sparqlClient.executeSelectQuery(queryTask, resultSet -> {
            if (!resultSet.hasNext()) {
                throw new RuntimeException("Input container graph not found");
            }
            List<DataContainer> graphUris = new ArrayList<>();
            resultSet.forEachRemaining(
                    r -> graphUris.add(DataContainer.builder()
                            .graphUri(r.getResource("graph").getURI())
                            .build()));
            return graphUris;
        }, highLoadSparqlEndpoint, true);
    }

    public void appendTaskError(Task task, String message) {
        log.info("writing error '{}', to task {} for job {}", message, task.getId(),
                task.getJob());
        var id = uuid();
        var uri = Constants.ERROR_URI_PREFIX + id;

        Map<String, Object> parameters = Map.of("task", task, "id", id, "uri", uri, "message",
                ofNullable(message).orElse("Unexpected error"));
        var queryStr = queryStore.getQueryWithParameters("appendTaskError", parameters);

        sparqlClient.executeUpdateQuery(queryStr, defaultSparqlEndpoint, true);
    }
}
