package org.vortex.executor;

import org.jdeferred.impl.DefaultDeferredManager;
import org.vortex.Settings;
import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Lists;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.executor.state.HttpStateCapture;
import org.vortex.executor.state.StateCapture;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class TargetExecutor implements Executor<TargetExecutor, Pair<VQuery, VTarget>> {

    private List<Pair<VQuery, VTarget>> tasks;
    private StateCapture stateCapture;
    private Settings settings;
    protected StructuredLog LOGGER = new StructuredLog(this.getClass());


    public TargetExecutor(StateCapture stateCapture) {
        this.stateCapture = stateCapture;
        this.settings = settings;
        tasks = Lists.list();
    }

    @Override
    public TargetExecutor withStep(Pair<VQuery, VTarget> taskQueryTargetPair) {
        tasks.add(taskQueryTargetPair);
        return this;
    }

    public TargetExecutor withStep(VQuery taskQuery, VTarget target) {
        return withStep(Pair.pair(taskQuery, target));
    }

    private Function<Pair<VQuery, VTarget>, Pair<String, String>> executeTask() {
        final DefaultDeferredManager defaultDeferredManager = new DefaultDeferredManager();
        return taskQueryTargetPair -> {
            VQuery query = taskQueryTargetPair.first();
            VTarget target = taskQueryTargetPair.second();
            defaultDeferredManager
                    .when(target.execute(query))
                    .done(stateCapture)
                    .fail(stateCapture);
            return Pair.pair(query.info(), target.info());
        };
    }

    public Result submit() {
        LOGGER.info(Pair.pair("stage", "TargetSubmit"), Pair.pair("stage", "PreSubmit"), Pair.pair("stateCapture", stateCapture.info()));
        List<Pair<String, String>> submitted = tasks.stream().map(executeTask()).collect(Collectors.toList());
        return Result.success("Submitted", Maps.<String, Object>map(submitted.stream()));
    }

    public static TargetExecutor withDefaultCallbacks(Settings settings) {
        return new TargetExecutor(new HttpStateCapture(settings));
    }

}
