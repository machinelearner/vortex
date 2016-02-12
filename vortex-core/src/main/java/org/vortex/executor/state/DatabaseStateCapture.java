package org.vortex.executor.state;

import org.joda.time.DateTime;
import org.vortex.basic.StructuredLog;
import org.vortex.basic.primitive.Maps;
import org.vortex.basic.primitive.Pair;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.query.UpdateQuery;

import static org.vortex.basic.primitive.Pair.pair;

public class DatabaseStateCapture implements StateCapture {
    protected StructuredLog LOGGER = new StructuredLog();
    private String COLLECTION_NAME = "migrations.log";
    private VTarget target;

    public DatabaseStateCapture(VTarget target) {
        this.target = target;
    }

    public DatabaseStateCapture intoCollection(String collectionName){
        this.COLLECTION_NAME = collectionName;
        return this;
    }

    private String getId(Result result) {
        return result.result().containsKey("_id") ? (String) result.result().get("_id") : String.valueOf(DateTime.now().toDate().getTime());
    }

    @Override
    public void onDone(Result success) {
        String id = getId(success);
        LOGGER.info(Pair.pair("stage", "postExecution"), Pair.pair("_id", id));
        try {
            new UpdateQuery()
                    .into(COLLECTION_NAME)
                    .where("_id", id)
                    .object(Maps.<String, Object>map(success.result(), pair("status", success.get("status").toString()), pair("executedOn", DateTime.now().toDate())))
                    .upsert(true)
                    .executeAgainst(target)
                    .call();
        } catch (Exception e) {
            LOGGER.error(e, Pair.pair("stage", "successStateCapture"), Pair.pair("result", success));
        }
    }

    @Override
    public void onFail(Result failure) {
        try {
            new UpdateQuery()
                    .into(COLLECTION_NAME)
                    .where("_id", getId(failure))
                    .object(Maps.<String, Object>map(failure.result(), pair("status", failure.get("status").toString()), pair("executedOn", DateTime.now().toDate())))
                    .upsert(true)
                    .executeAgainst(target)
                    .call();
        } catch (Exception e) {
            LOGGER.error(e, Pair.pair("stage", "failStateCapture"), Pair.pair("result", failure));
        }
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void onFail(Throwable result) {
        LOGGER.error(result, Pair.pair("stage", "postExecution"));
        onFail(Result.failure(result.getMessage(), Maps.map()));
    }
}
