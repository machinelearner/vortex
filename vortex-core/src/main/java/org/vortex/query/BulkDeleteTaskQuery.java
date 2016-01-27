package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.basic.primitive.Maps;

import java.util.Map;
import java.util.concurrent.Callable;

public class BulkDeleteTaskQuery implements VQuery {
    private Map<String, Object> conditions;

    public BulkDeleteTaskQuery() {
        this.conditions = Maps.map();
    }

    /*
        This is a quick impl;
        Move towards an implementation like {@link Criteria} of spring-data-mongo which is more composition friendly
        and flexible in constructing sophisticated queries
     */
    public BulkDeleteTaskQuery condition(String key, String value) {
        conditions.put(key, value);
        return this;
    }

    public Map<String, Object> conditions() {
        return conditions;
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public JSONObject toJson() {
        Map<String, Map<String, Object>> asMap = Maps.map("conditions", conditions);
        return new JSONObject(asMap);
    }

    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        return target.delete(this);
    }
}
