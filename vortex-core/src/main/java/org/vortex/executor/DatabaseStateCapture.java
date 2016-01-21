package org.vortex.executor;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.help.Maps;
import org.vortex.query.UpdateQuery;

import static org.vortex.help.Pair.pair;

public class DatabaseStateCapture implements StateCapture {
    protected Logger LOGGER = LoggerFactory.getLogger(this.getClass());
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
        try {
            new UpdateQuery()
                    .into(COLLECTION_NAME)
                    .where("_id", getId(success))
                    .object(Maps.<String, Object>map(success.result(), pair("status", success.get("status").toString()), pair("executedOn", DateTime.now().toDate())))
                    .upsert(true)
                    .executeAgainst(target)
                    .call();
        } catch (Exception e) {
            LOGGER.error("Failed to capture flow", success);
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
            LOGGER.error("Failed to capture flow", failure);
        }
    }

    @Override
    public String info() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void onFail(Throwable result) {
        LOGGER.error("Failed to capture state", result);
        onFail(Result.failure(result.getMessage(), Maps.map()));
    }
}
