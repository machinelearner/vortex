package org.vortex.executor;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.vortex.Settings;
import org.vortex.domain.Result;
import org.vortex.domain.VQuery;
import org.vortex.domain.VTarget;
import org.vortex.basic.primitive.Maps;
import org.vortex.impl.target.MongoTarget;
import org.vortex.impl.target.OrientdbTarget;
import org.vortex.query.BulkDeleteTaskQuery;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static name.mlnkrishnan.shouldJ.ShouldJ.it;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class TargetExecutorTest {

    private Settings defaults;

    CountDownLatch countDown;
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8090);

    @Before
    public void setUp() throws Exception {
        defaults = Settings.overrideDefaults(new Settings() {{
            put("mongo.db.name", "vortex_test");
            put("http.notification.url", "http://localhost:8090/notify");
        }});
        countDown = new CountDownLatch(1);
        stubFor(post(urlEqualTo("/notify"))
                .willReturn(aResponse().withStatus(200)));
    }

    @After
    public void tearDown() throws InterruptedException {
        //Wait before shutting down wiremock
        Thread.sleep(500);
    }

    @Test
    public void shouldCreateAsyncExecutor() throws InterruptedException {
        Result result = TargetExecutor.withDefaultCallbacks(defaults)
                .withStep(new BulkDeleteTaskQuery(), new MongoTarget(defaults))
                .submit();

        it(result.isSuccess()).shouldBeTrue();
    }

    @Test
    public void shouldGetAllStepsInExecutorAsSubmittedStatusAsPartOfResult() {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = new BulkDeleteTaskQuery();
        MongoTarget mongoTarget = new MongoTarget(defaults);
        OrientdbTarget orientdbTarget = new OrientdbTarget(defaults);

        Result result = TargetExecutor.withDefaultCallbacks(defaults)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .withStep(bulkDeleteTaskQuery, orientdbTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info(), bulkDeleteTaskQuery.info(), orientdbTarget.info()));
    }

    @Test
    public void shouldSubmitATaskWithTargetToPerformExecution() {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(VTarget.successCallable());

        Result result = TargetExecutor.withDefaultCallbacks(defaults)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info()));
    }

    @Test
    public void shouldSubmitATaskWithTargetAndCaptureSuccessState() throws InterruptedException {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(VTarget.successCallable());
        StateCapture mockStateCapture = mock(StateCapture.class);

        Result result = new TargetExecutor(mockStateCapture)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(1, TimeUnit.SECONDS);
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        verify(mockStateCapture).onDone(Result.success("No Op"));
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info()));
    }

    @Test
    public void shouldSubmitATaskWithTargetAndCaptureFailureState() throws InterruptedException {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(() -> {
            throw new RuntimeException("fail");
        });
        StateCapture mockStateCapture = mock(StateCapture.class);
        Result result = new TargetExecutor(mockStateCapture)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(1, TimeUnit.SECONDS);
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockStateCapture).onFail(throwableArgumentCaptor.capture());
        it(throwableArgumentCaptor).shouldNotBeNull();
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info()));
    }

    @Test
    public void shouldSubmitMultipleTaskWithTargetAndCaptureSuccessState() throws InterruptedException {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        OrientdbTarget orientdbTarget = mock(OrientdbTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(VTarget.successCallable());
        StateCapture mockStateCapture = mock(StateCapture.class);

        Result result = new TargetExecutor(mockStateCapture)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        countDown.await(1, TimeUnit.SECONDS);
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        verify(mockStateCapture, times(1)).onDone(Result.success("No Op"));
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info(), bulkDeleteTaskQuery.info(), orientdbTarget.info()));
    }

    @Test
    @Ignore("Thread/CPU bound test and is very sensitive to CI CPU availability")
    public void shouldSubmitMultipleTaskWithTargetAndCaptureFailureStateForFailures() throws InterruptedException {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        OrientdbTarget orientdbTarget = mock(OrientdbTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(VTarget.successCallable());
        when(orientdbTarget.execute(Matchers.<VQuery>any())).thenReturn(() -> {
            throw new RuntimeException("fail");
        });
        StateCapture mockStateCapture = mock(StateCapture.class);

        Result result = new TargetExecutor(mockStateCapture)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .withStep(bulkDeleteTaskQuery, orientdbTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        countDown.await(1, TimeUnit.SECONDS);
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        verify(orientdbTarget).execute(bulkDeleteTaskQuery);
        ArgumentCaptor<Throwable> throwableArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(mockStateCapture, times(1)).onDone(Result.success("No Op"));
        verify(mockStateCapture, times(1)).onFail(throwableArgumentCaptor.capture());
        it(throwableArgumentCaptor).shouldNotBeNull();
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info(), bulkDeleteTaskQuery.info(), orientdbTarget.info()));
    }


    @Test
    public void shouldSubmitMultipleTaskWithTargetAndCaptureSuccessStateForLongRunningTask() throws InterruptedException {
        BulkDeleteTaskQuery bulkDeleteTaskQuery = mock(BulkDeleteTaskQuery.class);
        MongoTarget mongoTarget = mock(MongoTarget.class);
        OrientdbTarget objectStoreTarget = mock(OrientdbTarget.class);
        when(mongoTarget.execute(Matchers.<VQuery>any())).thenReturn(VTarget.successCallable());
        when(objectStoreTarget.execute(Matchers.<VQuery>any())).thenReturn(() -> {
            Thread.sleep(1000);
            return Result.success("waited");
        });
        StateCapture mockStateCapture = mock(StateCapture.class);

        Result result = new TargetExecutor(mockStateCapture)
                .withStep(bulkDeleteTaskQuery, mongoTarget)
                .withStep(bulkDeleteTaskQuery, objectStoreTarget)
                .submit();

        it(result.isSuccess()).shouldBeTrue();
        verify(mongoTarget).execute(bulkDeleteTaskQuery);
        verify(objectStoreTarget).execute(bulkDeleteTaskQuery);
        verify(mockStateCapture, times(1)).onDone(Result.success("No Op"));
        verify(mockStateCapture, times(0)).onDone(Result.success("waited")); // Nothing was called
        countDown.await(3, TimeUnit.SECONDS);
        verify(mockStateCapture, times(1)).onDone(Result.success("waited")); // Now its complete
        it(result.result()).shouldBe(Maps.<String, Object>map(bulkDeleteTaskQuery.info(), mongoTarget.info(), bulkDeleteTaskQuery.info(), objectStoreTarget.info()));
    }

}
