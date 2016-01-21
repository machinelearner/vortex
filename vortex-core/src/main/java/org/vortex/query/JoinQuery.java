package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.executor.ExecutorException;
import org.vortex.help.Maps;
import org.vortex.impl.target.GraphTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * <b>Still Evolving<b/>
 * Simplest form of expressing joins. Especially Open Joins - Left and Right Joins expressed as directions in and our respectively.
 * Its still a bit ugly that the nomenclature is missed-used. Bound to change soon
 */
public class JoinQuery implements VQuery, Cloneable {
    private List<Map<String, Object>> conditions;
    private String relation;
    private String entity;
    private String direction;

    public JoinQuery() {
        this.direction = "out";
        this.conditions = new ArrayList<>();
    }

    public JoinQuery relation(String relation, String entity) {
        this.relation = relation;
        this.entity = entity;
        return this;
    }
    public JoinQuery on(String relation) {
        this.relation = relation;
        return this;
    }

    public JoinQuery from(String entity) {
        this.entity = entity;
        return this;
    }

    public JoinQuery condition(Map<String, Object> condition) {
        this.conditions.add(condition);
        return this;
    }

    public JoinQuery conditions(List<Map<String, Object>> conditions) {
        this.conditions.addAll(conditions);
        return this;
    }

    public JoinQuery in() {
        this.direction = "in";
        return this;
    }

    public JoinQuery out() {
        this.direction = "out";
        return this;
    }

    public List<Map<String, Object>> conditions() {
        return conditions.stream().distinct().collect(Collectors.toList());
    }

    public String direction() {
        return direction;
    }

    public String entity() {
        return entity;
    }

    public String relation() {
        return relation;
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public JSONObject toJson() {
        Map<String, Object> asMap = Maps.map("condition", conditions(), "relation", relation(), "direction", direction());
        return new JSONObject(asMap);
    }

    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        GraphTarget graphTarget = (GraphTarget) target;
        return graphTarget.connectedList(this);
    }

    @Override
    public synchronized JoinQuery clone() {
        try {
            return (JoinQuery) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new ExecutorException("Connected Query is clone friendly but, something wrong happened", e);
        }
    }
}
