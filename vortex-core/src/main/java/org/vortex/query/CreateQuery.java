package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.help.Maps;
import org.vortex.help.Pair;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;


public class CreateQuery implements VQuery {

    private final Map<String, Object> conditions;
    private ArrayList whereClauses;
    private Map<String, Object> createObject;

    public CreateQuery() {
        this.conditions = Maps.map();
        this.whereClauses = new ArrayList<>();
        this.createObject = Maps.map();
    }

    public CreateQuery into(String into) {
        conditions.put("clauses.into", into);
        return this;
    }

    public CreateQuery where(String key, Object value) {
        whereClauses.add(Pair.of(key, value));
        return this;
    }

    public CreateQuery object(Map<String, Object> object) {
        this.createObject = object;
        return this;
    }

    public String into() {
        return (String) conditions.get("clauses.into");
    }

    public Map<String, Object> where() {
        return Maps.map(whereClauses.stream());
    }

    public Map<String, Object> object() {
        return createObject;
    }

    public Map<String, Object> conditions() {
        conditions.put("clause.into", into());
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
        return target.create(this);
    }
}
