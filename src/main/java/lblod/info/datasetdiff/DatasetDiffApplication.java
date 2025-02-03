// Source code is decompiled from a .class file using FernFlower decompiler.
package lblod.info.datasetdiff;

import com.github.jsonldjava.shaded.com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import mu.semte.ch.lib.handler.DefaultExceptionHandler;
import mu.semte.ch.lib.utils.SparqlClient;
import mu.semte.ch.lib.utils.SparqlQueryStore;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.CaseUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.Resource;

@SpringBootApplication
@Slf4j
@Import({ SparqlClient.class, DefaultExceptionHandler.class })
public class DatasetDiffApplication {
    @Value("${sparql.queryStore.path:classpath*:sparql}/*.sparql")
    private Resource[] queries;

    @Bean
    public SparqlQueryStore sparqlQueryLoader() {
        log.info("Adding {} queries to the store", this.queries.length);
        Map<String, String> queriesMap = (Map) Arrays.stream(this.queries).map((r) -> {
            try {
                String key = CaseUtils.toCamelCase(FilenameUtils.removeExtension(r.getFilename()), false,
                        new char[] { '-' });
                return Maps.immutableEntry(key, IOUtils.toString(r.getInputStream(), StandardCharsets.UTF_8));
            } catch (IOException var2) {
                throw new RuntimeException(var2);
            }
        }).peek((e) -> {
            log.info("query {} added to the store", e.getKey());
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        return () -> {
            return queriesMap;
        };
    }

    public static void main(String[] args) {
        SpringApplication.run(DatasetDiffApplication.class, args);
    }
}
