package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.basic.primitive.Maps;
import org.vortex.impl.target.GraphTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


public class RelateQuery implements VQuery, Cloneable {
    private String fromEntity;
    private String relationship;
    private String toEntity;
    private Map<String, Object> toCondition;
    private List<Map<String, Object>> fromConditions;

    public RelateQuery() {
        this.fromConditions = new ArrayList<>();
    }

    public RelateQuery relation(String fromEntity, String relationship, String toEntity) {
        this.fromEntity = fromEntity;
        this.relationship = relationship;
        this.toEntity = toEntity;
        return this;
    }

    public String relation() {
        return relationship;
    }

    public RelateQuery toCondition(Map<String, Object> condition) {
        this.toCondition = condition;
        return this;
    }

    public Map<String, Object> toCondition() {
        return toCondition;
    }

    public RelateQuery fromCondition(Map<String, Object> condition) {
        fromConditions.add(condition);
        return this;
    }

    public RelateQuery fromConditions(List<Map<String, Object>> conditions) {
        this.fromConditions.addAll(conditions);
        return this;
    }

    public List<Map<String, Object>> fromConditions() {
        return fromConditions.stream().distinct().collect(Collectors.toList());
    }

    public String fromEntity() {
        return fromEntity;
    }

    public String toEntity() {
        return toEntity;
    }


    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public JSONObject toJson() {
        Map<String, Object> map = Maps.map("from", fromEntity(), "toEntity", toEntity(), "relation", relation(), "fromCondition", fromConditions(), "toCondition", toCondition());
        return new JSONObject(map);
    }

    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        GraphTarget graphTarget = (GraphTarget) target;
        return graphTarget.relate(this);
    }
}
