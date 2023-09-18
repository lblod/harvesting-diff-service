package lblod.info.datasetdiff;

import org.apache.jena.rdf.model.Model;

public record ModelByDerived(String derivedFrom, Model model) {
}
