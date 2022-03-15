package ch.heig.mac;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final  Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> possibleSpreadCounts() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> carelessPeople() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sociallyCareful() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> peopleToInform() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> setHighRisk() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> healthyCompanionsOf(String name) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public Record topSickSite() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sickFrom(List<String> names) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }
}
