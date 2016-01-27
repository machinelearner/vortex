package org.vortex.query;

import org.json.simple.JSONObject;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.executor.ExecutorException;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.impl.target.MongoTarget;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


public class ListQuery implements VQuery, Cloneable {
    public static final int DEFAULT_PAGE_SIZE = 50;
    private Map<String, Object> conditions;
    private List<Pair<String, Object>> whereClauses;
    private List<Pair<String, Object>> sortOrderedClauses;

    public enum SortOrder {
        ASC, DESC
    }

    public ListQuery() {
        this.conditions = Maps.map();
        this.whereClauses = new ArrayList<>();
        this.sortOrderedClauses = new ArrayList<>();
    }

    public ListQuery where(String key, Object value) {
        whereClauses.add(Pair.of(key, value));
        return this;
    }

    @Deprecated
    public ListQuery where(String key, String value) {
        whereClauses.add(Pair.of(key, value));
        return this;
    }

    public ListQuery from(String from) {
        conditions.put("clause.from", from);
        return this;
    }

    public ListQuery sortBy(String sortBy, String sortOrder) {
        sortOrderedClauses.add(Pair.pair(sortBy, sortOrder));
        return this;
    }

    public ListQuery sortBy(String sortBy, SortOrder sortOrder) {
        sortOrderedClauses.add(Pair.pair(sortBy, sortOrder));
        return this;
    }

    public ListQuery pageSize(int size) {
        conditions.put("clause.page.size", size);
        return this;
    }

    public ListQuery pageStart(String field, Object value) {
        conditions.put("clause.page.start", Pair.pair(field, value));
        return this;
    }

    /**
     * All clauses part of the list query
     * Includes - from, where, sort
     */
    public Map<String, Object> conditions() {
        conditions.put("clause.where", where());
        conditions.put("clause.sort", sort());
        return conditions;
    }

    /**
     * Equivalent to SQL 'from' clause; Should be used appropriately for different target;
     * E.g:
     * 1. Mongo: from is usually a collection name
     * 2. ObjectStore: from is usually container
     * 3. OrientDb: from is usually vertex, class etc
     * <p/>
     * key name: <i>clause.from</i>
     * value: collection name or class name or table name
     */
    public String from() {
        return (String) conditions.get("clause.from");
    }

    /**
     * This is a simple impl;
     * Move towards an implementation like {@link Criteria} of spring-data-mongo which is more composition friendly
     * and flexible in constructing sophisticated queries
     * <p/>
     * Very hesitant to introduce operators as they will become quite complex to manage eventually especially
     * with heterogeneous stores
     */
    public Map<String, Object> where() {
        return Maps.map(whereClauses.stream());
    }

    /**
     * Sort expresses multi level/secondary sort capabilities. List of Map containing sort by and sort order.
     * The level or precedence is represented by the list i.e first clause is the primary sort option,
     * second is for collisions over first, third is for collisions over second and so on..
     * <p/>
     * Sort Order values for different targets
     * E.g:
     * 1. Mongo: -1,1 indicating ascending or descending
     * 2. ObjectStore: NA
     * 3. OrientDb: asc, desc
     * <p/>
     * Sort By value: field name
     */
    public List<Pair<String, Object>> sort() {
        return sortOrderedClauses;
    }

    /**
     * Size of the results returned by target as part of query execution
     * E.g:
     * 1. Mongo: limit value
     * 2. ObjectStore: PageSize
     * 3. Elasticsearch: PageSize
     * 4. Orientdb: Limit
     */
    public Integer pageSize() {
        return (Integer) Maps.get(conditions, "clause.page.size", DEFAULT_PAGE_SIZE);
    }

    /**
     * Start condition for pagination. This is built and expressed separately due the following two reasons:
     * <ol>
     * <li>
     * Pagination which is natively supported by many store eventually become performance bottleneck as deep pages
     * will cause memory issues for the store(especially when its distributed). One of the best ways to achieve
     * pagination without using native support is to know the start condition and the page limit. This can
     * be controlled separately to hit indices and hence not be memory heavy
     * </li>
     * <li>
     * The pagination start is technically another *where* condition, but in our abstraction of {@link VQuery},
     * introducing support for different operators seems like an overkill given that we are trying to do this(for now),
     * only to achieve pagination. It made sense for each *VTarget* to understand this explicitly and make use of it as
     * an optimization step. Hence, the operator is implicit and is left to the target to implement it appropriately.
     * E.g: In {@link MongoTarget} we are using $gt as the operator to achieve this
     * </li>
     * </ol>
     * <b>Optimization Parameter for Pagination</b>
     */
    public Pair<String, Object> pageStart() {
        return (Pair) conditions.get("clause.page.start");
    }

    public String pageStartField() {
        return ((Pair<String, Object>) conditions.get("clause.page.start")).first();
    }

    /**
     * @param target VTarget to execute the query against
     * @return function which will eventually be executed by the deferred manager to get results
     */
    @Override
    public Callable<Result> executeAgainst(VTarget target) {
        return target.list(this);
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
    public synchronized ListQuery clone() {
        try {
            return (ListQuery) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new ExecutorException("List Query is clone friendly but, something wrong happened", e);
        }
    }

    public static List<Map<String, Object>> list(Result result) {
        if (result.isSuccess())
            return (List<Map<String, Object>>) result.result().get("result");
        throw new ExecutorException("Cannot extract results from unsuccessful query!");
    }
}
