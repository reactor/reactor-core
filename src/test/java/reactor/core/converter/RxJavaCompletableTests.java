package reactor.core.converter;

import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.core.test.TestSubscriber;
import rx.Completable;
import rx.Observable;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

/**
 * @author Joao Pedro Evangelista
 */
public class RxJavaCompletableTests {

    private Completable completable;


    @Before
    public void setUp() throws Exception {
        completable = Completable.fromObservable(Observable.just(1,2,3,4,5));
    }

    @Test
    public void shouldConvertACompletableIntoMonoVoid() throws Exception {
        Mono<Void> printableMono = RxJava1CompletableConverter.toPublisher(completable);
        assertThat(printableMono, is(notNullValue()));
        TestSubscriber
                .subscribe(printableMono)
                .assertNoValues()
                .assertComplete();
    }


    @Test
    public void completableWithErrorIntoMono() throws Exception {
        Mono<Void> voidMono = RxJava1CompletableConverter.toPublisher(Completable.fromAction(() -> {
            throw new IllegalStateException("This should not happen"); //something internal happened
        }));
        TestSubscriber
                .subscribe(voidMono)
                .assertNoValues()
                .assertError(IllegalStateException.class)
                .assertTerminated()
                .assertNotComplete();
    }

    @Test
    public void shouldConvertAMonoIntoCompletable() throws Exception {
        Completable completable = RxJava1CompletableConverter.fromPublisher(Mono.just(1));
        Throwable maybeErrors = completable.get();
        assertThat(maybeErrors, is(nullValue()));
    }
}
