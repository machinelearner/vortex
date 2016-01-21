package org.vortex.domain;

import org.json.simple.JSONObject;

import java.util.concurrent.Callable;

public interface VQuery {
    String info();
    JSONObject toJson();
    Callable<Result> executeAgainst(VTarget target);

}
