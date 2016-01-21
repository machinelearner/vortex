package org.vortex.executor;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.vortex.domain.Result;
import org.vortex.domain.VTarget;
import org.vortex.help.Maps;
import org.vortex.query.UpdateQuery;

import static name.mlnkrishnan.shouldJ.ShouldJ.it;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.vortex.help.Pair.pair;

public class DatabaseStateCaptureTest {

    @Mock
    VTarget target;

    DatabaseStateCapture databaseStateCapture;

    @Before
    public void setUp() throws Exception {
        initMocks(this);
        databaseStateCapture = new DatabaseStateCapture(target);
    }



    @Test
    public void shouldSendNotificationOnSuccess() {
        Result done = Result.success("done", Maps.<String, Object>map("_id", "id1"));
        String into = "coll1";

        databaseStateCapture.intoCollection(into).onDone(done);

        UpdateQuery upsert = new UpdateQuery()
                .into(into)
                .where("_id", done.result().get("_id"))
                .object(Maps.<String, Object>map(done.result(), pair("status", Result.Status.SUCCESS.toString()), pair("executedOn", DateTime.now().toDate())))
                .upsert(true);

        ArgumentCaptor<UpdateQuery> captor = ArgumentCaptor.forClass(UpdateQuery.class);
        verify(target).update(captor.capture());
        it(captor.getValue().toJson()).shouldBe(upsert.toJson());

    }

    @Test
    public void shouldSendNotificationOnFailure() {
        Result done = Result.failure("done", Maps.<String, Object>map("_id", "id1"));
        String into = "coll1";

        databaseStateCapture.intoCollection(into).onFail(done);

        UpdateQuery upsert = new UpdateQuery()
                .into(into)
                .where("_id", done.result().get("_id"))
                .object(Maps.<String, Object>map(done.result(), pair("status", Result.Status.FAILURE.toString()), pair("executedOn", DateTime.now().toDate())))
                .upsert(true);

        ArgumentCaptor<UpdateQuery> captor = ArgumentCaptor.forClass(UpdateQuery.class);
        verify(target).update(captor.capture());
        it(captor.getValue().toJson()).shouldBe(upsert.toJson());
    }

    @Test
    public void shouldGenerateIdWhenNotPassed() {
        Result done = Result.failure("done", Maps.<String, Object>map("baaa", "id1"));
        String into = "coll1";

        databaseStateCapture.intoCollection(into).onFail(done);

        UpdateQuery upsert = new UpdateQuery()
                .into(into)
                .where("_id", done.result().get("_id"))
                .object(Maps.<String, Object>map(done.result(), pair("status", Result.Status.FAILURE.toString()), pair("executedOn", DateTime.now().toDate())))
                .upsert(true);

        ArgumentCaptor<UpdateQuery> captor = ArgumentCaptor.forClass(UpdateQuery.class);
        verify(target).update(captor.capture());
        it(captor.getValue().where()).shouldHaveKey("_id");
    }
}