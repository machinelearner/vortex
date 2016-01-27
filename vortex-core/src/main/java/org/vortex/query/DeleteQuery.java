package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Stream;


public class DeleteQuery  implements VQuery {

    private Map<String, Object> conditions;
    private Stream<Pair<String, Object>> whereClauses;

    public DeleteQuery() {
        this.conditions = Maps.map();
        this.whereClauses = Stream.of();
    }

    public DeleteQuery where(String key, String value) {
        whereClauses = Stream.concat(whereClauses, Stream.of(Pair.<String, Object>pair(key, value)));
        return this;
    }

    public DeleteQuery from(String from) {
        conditions.put("clause.from", from);
        return this;
    }

    public String from() {
        return (String) conditions.get("clause.from");
    }

    public Map<String, Object> where() {
        return Maps.map(whereClauses);
    }

    public Map<String, Object> conditions() {
        conditions.put("clause.where", where());
        return conditions;
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public JSONObject toJson() {
        Map<String, Map<String, Object>> asMap = Maps.map("conditions", conditions());
        return new JSONObject(asMap);
    }

    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        return target.delete(this);
    }
}
