package ch.heig.mac;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final Logger LOGGER = Logger.getLogger(Requests.class.getName());
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
        var result = driver.session().run(
                "MATCH (p:Person{healthstatus: 'Sick'})-->(v1:Visit)" +
                "-->(pl:Place)<--(v2:Visit)<--(h:Person{healthstatus: 'Healthy'}) \n" +
                "WHERE p.confirmedtime < v2.starttime\n" +
                "RETURN p.name AS sickName, COUNT(h) AS nbHealthy");

        return result.list();
    }

    public List<Record> carelessPeople() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<Record> sociallyCareful() {
        var result = driver.session().run(
                "MATCH (sick:Person{healthstatus: 'Sick'})\n" +
                        "WHERE NOT EXISTS {\n" +
                        "(sick)-[v:VISITS]->(p:Place{type:'Bar'})\n" +
                        "WHERE sick.confirmedtime <= v.starttime\n" +
                        "} RETURN sick.name AS sickName"
        );
        return result.list();
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
        Map<String, Object> params = new HashMap<>();
        params.put("listOfNames", names);

        var result = driver.session().run(
                "MATCH (p:Person {healthstatus:'Sick'})\n" +
                "WHERE p.name IN $listOfNames\n" +
                "RETURN p.name AS sickName",
                params
        );

        return result.list();
    }
}
