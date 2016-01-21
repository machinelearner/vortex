package org.vortex.executor;

import org.jdeferred.impl.DefaultDeferredManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.help.Lists;
import org.vortex.help.Maps;
import org.vortex.help.Pair;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class TargetExecutor implements Executor<TargetExecutor, Pair<VQuery, VTarget>> {

    private List<Pair<VQuery, VTarget>> tasks;
    private StateCapture stateCapture;
    private Settings settings;
    protected Logger LOGGER = LoggerFactory.getLogger(this.getClass());


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
        return new Function<Pair<VQuery, VTarget>, Pair<String, String>>() {
            @Override
            public Pair<String, String> apply(Pair<VQuery, VTarget> taskQueryTargetPair) {
                VQuery query = taskQueryTargetPair.first();
                VTarget target = taskQueryTargetPair.second();
                defaultDeferredManager
                        .when(target.execute(query))
                        .done(stateCapture)
                        .fail(stateCapture);
                return Pair.pair(query.info(), target.info());
            }
        };
    }

    public Result submit() {
        LOGGER.info("VTarget Executor submitted with {} way of capturing state", stateCapture.info());
        List<Pair<String, String>> submitted = tasks.stream().map(executeTask()).collect(Collectors.toList());
        return Result.success("Submitted", Maps.<String, Object>map(submitted.stream()));
    }

    public static TargetExecutor withDefaultCallbacks(Settings settings) {
        return new TargetExecutor(new HttpStateCapture(settings));
    }

}
