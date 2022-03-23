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
        var result = driver.session().run(
                "MATCH (pS:Person{healthstatus:'Sick'})-->(vS:Visit)-->(Place)<--(vH:Visit)<--(pH:Person{healthstatus:'Healthy'})\n" +
                        "WHERE pS.confirmedtime <= vS.starttime AND pS.confirmedtime <= vH.starttime\n" +
                        "RETURN DISTINCT pS.name as sickName");
        return result.list();
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
        var result = driver.session().run(
                "MATCH (sick:Person{healthstatus: 'Sick'})-[v:VISITS]->(p:Place)\n" +
                        "WHERE v.starttime > sick.confirmedtime\n" +
                        "WITH sick.name as sickName, count(DISTINCT p.name) as nbPlaces\n" +
                        "WHERE nbPlaces > 10\n" +
                        "RETURN sickName, nbPlaces\n" +
                        "ORDER BY nbPlaces DESC;"
        );

        return result.list();
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
        // Une autre version avec CALL
        /*
        "MATCH (sick:Person{healthstatus: 'Sick'})-[v1:VISITS]->(p1:Place)\n" +
                "CALL {\n" +
                "    WITH v1, p1\n" +
                "    MATCH (healthy:Person{healthstatus: 'Healthy'})-[v2:VISITS]->(p2:Place{id: p1.id})\n" +
                "    WITH duration.between(apoc.coll.max([v1.starttime, v2.starttime]), apoc.coll.min([v1.endtime, v2.endtime])) as overlap, healthy\n" +
                "    WHERE overlap.hours >= 2\n" +
                "    RETURN healthy.name AS toInform\n" +
                "}\n" +
                "RETURN sick.name AS sickName, collect(toInform) as peopleToInform"
         */
        var result = driver.session().run(
                "MATCH (sick:Person{healthstatus: 'Sick'})-[v1:VISITS]->(p1:Place)<-[v2:VISITS]-(healthy:Person{healthstatus: 'Healthy'})\n" +
                        "WHERE duration.between(apoc.coll.max([v1.starttime, v2.starttime]), apoc.coll.min([v1.endtime, v2.endtime])).hours >= 2\n" +
                        "RETURN sick.name AS sickName, collect(DISTINCT healthy.name) as peopleToInform\n" +
                        "ORDER BY peopleToInform"
        );

        return result.list();
    }

    public List<Record> setHighRisk() {
        // Une autre version avec CALL
        /*
         * "MATCH (sick:Person{healthstatus: 'Sick'})-[v1:VISITS]->(p1:Place)\n" +
                        "CALL {\n" +
                        "    WITH v1, p1\n" +
                        "    MATCH (healthy:Person{healthstatus: 'Healthy'})-[v2:VISITS]->(p2:Place{id: p1.id})\n" +
                        "    WITH duration.between(apoc.coll.max([v1.starttime, v2.starttime]), apoc.coll.min([v1.endtime, v2.endtime])) as overlap, healthy\n" +
                        "    WHERE overlap.hours >= 2\n" +
                        "    SET healthy.risk = 'high'\n" +
                        "    RETURN healthy.name as highRiskName\n" +
                        "}\n" +
                        "RETURN highRiskName;"
         */
        var result = driver.session().run(
                "MATCH (sick:Person{healthstatus: 'Sick'})-[v1:VISITS]->(p1:Place)<-[v2:VISITS]-(healthy:Person{healthstatus: 'Healthy'})\n" +
                        "WHERE duration.between(apoc.coll.max([v1.starttime, v2.starttime]), apoc.coll.min([v1.endtime, v2.endtime])).hours >= 2\n" +
                        "SET healthy.risk = 'high'\n" +
                        "RETURN healthy.name as highRiskName"
        );

        return result.list();
    }

    public List<Record> healthyCompanionsOf(String name) {
        Map<String, Object> params = new HashMap<>();
        params.put("name", name);
        //P-V-P-V-P-V-P (P = Person, V = Visit, - = jump) so min 2 jumps and max 6 jumps
        var result = driver.session().run(
                "MATCH (p:Person {name: $name})-[:VISITS*2..6]-(c:Person {healthstatus:'Healthy'})\n" +
                        "RETURN DISTINCT c.name AS healthyName",
                params
        );

        return result.list();
    }

    public Record topSickSite() {
        var result = driver.session().run(
                "MATCH (sick:Person{healthstatus: 'Sick'})-[v:VISITS]->(p:Place)\n" +
                        "WHERE v.starttime <= sick.confirmedtime\n" +
                        "WITH p, count(sick) AS nbOfSickVisits\n" +
                        "RETURN p.type AS placeType, nbOfSickVisits\n" +
                        "ORDER BY nbOfSickVisits DESC\n" +
                        "LIMIT 1;"
        );

        return result.list().get(0);
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
