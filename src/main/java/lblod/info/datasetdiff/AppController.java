package lblod.info.datasetdiff;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;
import mu.semte.ch.lib.dto.Delta;

@RestController
@Slf4j
public class AppController {
  private final AppService appService;

  public AppController(AppService appService) {
    this.appService = appService;
  }

  @PostMapping("/delta")
  public ResponseEntity<Void> delta(@RequestBody List<Delta> deltas, HttpServletRequest request) {
    var entries = deltas.stream().findFirst()
        .map(delta -> delta.getInsertsFor(Constants.SUBJECT_STATUS, Constants.STATUS_SCHEDULED))
        .orElseGet(List::of);

    if (entries.isEmpty()) {
      log.error("Delta dit not contain potential tasks that are ready for filtering, awaiting the next batch!");
      return ResponseEntity.noContent().build();

    }

    // NOTE: we don't wait as we do not want to keep hold off the connection.
    entries.forEach(appService::runAsync);

    return ResponseEntity.ok().build();
  }
}
