package org.vortex.domain;

import org.json.simple.JSONObject;
import org.vortex.basic.primitive.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Result extends HashMap<String, Object> {

    public Boolean isSuccess() {
        return get("status").equals(Status.SUCCESS);
    }

    public Boolean isFailure() {
        return get("status").equals(Status.FAILURE);
    }

    public Map<String, Object> result() {
        return (Map<String, Object>) get("result");
    }

    public enum Status {
        FAILURE, SUCCESS
    }

    public static Result success(String message) {
        Result successResult = new Result();
        successResult.put("status", Status.SUCCESS);
        successResult.put("message", message);
        return successResult;
    }

    public static Result success(String message, Map<String, Object> result) {
        Result successResult = new Result();
        successResult.put("status", Status.SUCCESS);
        successResult.put("message", message);
        successResult.put("result", result);
        return successResult;
    }

    public static Result failure(Map<String, List<String>> errors) {
        Result failureResult = new Result();
        failureResult.put("status", Status.FAILURE);
        failureResult.put("result", errors);
        return failureResult;
    }

    @Deprecated
    public static Result failure(List<String> errors) {
        Result failureResult = new Result();
        failureResult.put("status", Status.FAILURE);
        failureResult.put("result", Maps.map("errors", errors));
        return failureResult;
    }

    public static Result failure(String message, Map<String, Object> errorResult) {
        Result failureResult = new Result();
        failureResult.put("status", Status.FAILURE);
        failureResult.put("message", message);
        failureResult.put("result", errorResult);
        return failureResult;
    }

    public JSONObject toJson() {
        return new JSONObject(this);
    }
}

