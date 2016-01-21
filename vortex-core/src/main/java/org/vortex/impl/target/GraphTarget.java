package org.vortex.impl.target;

import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.query.JoinQuery;
import org.vortex.query.RelateQuery;

import java.util.concurrent.Callable;

public abstract class GraphTarget extends VTarget {

    public GraphTarget(Settings settings) {
        super(settings);
    }

    public abstract Callable<Result> connectedList(JoinQuery joinQuery);

    public abstract Callable<Result> relate(RelateQuery relateQuery);
}
