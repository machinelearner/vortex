package org.vortex.domain;

import org.vortex.query.ListQuery;

import java.util.Map;

public class QueryResult {

    private final ListQuery listQuery;
    private final Map<String, Object> aResult;

    public QueryResult(ListQuery listQuery, Map<String, Object> aResult) {
        this.listQuery = listQuery;
        this.aResult = aResult;
    }

    public Map<String, Object> result(){
        return aResult;
    }

    public String from(){
        return listQuery.from();
    }

    public ListQuery responsibleQuery(){
        return listQuery;
    }
}
