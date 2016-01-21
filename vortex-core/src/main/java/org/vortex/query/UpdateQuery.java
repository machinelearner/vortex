package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.help.Maps;
import org.vortex.help.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


public class UpdateQuery implements VQuery {
    private final Map<String, Object> conditions;
    private List whereClauses;
    private Map<String, Object> updateObject;

    public UpdateQuery() {
        this.conditions = Maps.map();
        this.whereClauses = new ArrayList<>();
    }

    public UpdateQuery into(String into) {
        conditions.put("clause.into", into);
        return this;
    }

    public UpdateQuery where(String key, Object value) {
        whereClauses.add(Pair.of(key, value));
        return this;
    }

    public UpdateQuery object(Map<String, Object> object) {
        this.updateObject = object;
        return this;
    }

    public UpdateQuery upsert(boolean upsert) {
        conditions.put("clause.upsert", upsert);
        return this;
    }

    public Map<String, Object> object() {
        return updateObject;
    }

    public Boolean upsert() {
        return (Boolean) Maps.get(conditions, "clause.upsert", Boolean.FALSE);
    }

    public String into() {
        return (String) conditions.get("clause.into");
    }

    public Map<String, Object> where() {
        return Maps.map(whereClauses.stream());
    }

    public Map<String, Object> conditions() {
        conditions.put("clause.into", into());
        conditions.put("clause.where", where());
        return conditions;
    }

    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        return target.update(this);
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
}
