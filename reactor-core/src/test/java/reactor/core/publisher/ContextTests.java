/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReference;

import org.hamcrest.Matchers;
import org.junit.Test;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Stephane Maldini
 */
public class ContextTests {

	@Test
	public void contextPassing() throws InterruptedException {
		AtomicReference<Context> c = new AtomicReference<>();
		AtomicReference<Context> innerC = new AtomicReference<>();

		Flux.range(1, 1000)
		    //ctx: test=baseSubscriber
		    //return: test=baseSubscriber_range
		    .contextMap(ctx -> ctx.put("test", ctx.get("test") + "_range"))
		    .log()
		    .flatMapSequential(d -> Mono.just(d)
		                                //ctx: test=baseSubscriber_range_take
		                                //return: old (discarded since inner)
		                                .contextMap(ctx -> {
			                                if (innerC.get() == null) {
				                                innerC.set(ctx.put("test", ctx.get("test") + "_innerFlatmap"));
			                                }
			                                return ctx;
		                                })
		                                .log())
		    .map(d -> d)
		    .take(10)
		    //ctx: test=baseSubscriber_range
		    //return: test=baseSubscriber_range_take
		    .contextMap(ctx -> ctx.put("test", ctx.get("test") + "_take"))
		    .log()
		    .subscribe(new BaseSubscriber<Integer>() {
			    @Override
			    public Context currentContext() {
				    return Context.empty()
				                  .put("test", "baseSubscriber");
			    }

			    @Override
			    public void onContextUpdate(Context context) {
				    c.set(context);
			    }
		    });

		assertThat(c.get()
		            .get("test"), is("baseSubscriber_range_take"));

		assertThat(c.get()
		            .get("test2"), Matchers.nullValue());

		assertThat(innerC.get()
		                 .get("test"), is("baseSubscriber_range_take_innerFlatmap"));
	}

	@Test
	public void contextPassing2() throws InterruptedException {
		AtomicReference<String> innerC = new AtomicReference<>();

		Flux.range(1, 1000)
		    .contextMap(ctx -> ctx.put("test", "foo"))
		    .log()
		    .flatMapSequential(d ->
				    Mono.just(d)
				        .contextMap(ctx -> {
					        if (innerC.get() == null) {
						        innerC.set(""+ ctx.get("test") + ctx.get("test2"));
					        }
					        return ctx;
				        })
				        .log())
		    .map(d -> d)
		    .take(10)
		    .contextMap(ctx -> ctx.put("test2", "bar"))
		    .log()
		    .subscribe();

		assertThat(innerC.get(), is("foobar"));
	}

	@Test
	public void contextGet() throws InterruptedException {

		StepVerifier.create(Flux.range(1, 1000)
		                        .contextMap(ctx -> ctx.put("test", "foo"))
		                        .log()
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test") + "" + d)
		                        .skip(3)
		                        .take(3)
		                        .contextMap(ctx -> ctx.put("test2", "bar"))
		                        .map(d -> d)
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test2") + "" + d)
		                        .log())
		            .expectNext("barfoo4")
		            .expectNext("barfoo5")
		            .expectNext("barfoo6")
		            .verifyComplete();
	}

	@Test
	public void currentContext() throws InterruptedException {

		StepVerifier.create(Mono.just("foo")
		                        .flatMap(d -> Mono.currentContext()
		                                          .map(c -> d + c.get(Integer.class)))
		                        .contextStart(ctx ->
				                        ctx.put(Integer.class, ctx.get(Integer.class) + 1))
		                        .flatMapMany(Mono::just)
		                        .contextStart(ctx -> ctx.put(Integer.class, 0))
		                        .log())
		            .expectNext("foo1")
		            .verifyComplete();
	}

	@Test
	public void currentContextEmpty() throws InterruptedException {

		StepVerifier.create(Mono.just("foo")
		                        .flatMap(d -> Mono.currentContext()
		                                          .map(c -> d + c.get(Integer.class)))
		                        .log())
		            .verifyComplete();
	}

	@Test
	public void contextGetHide() throws InterruptedException {

		StepVerifier.create(Flux.range(1, 1000)
		                        .hide()
		                        .contextMap(ctx -> ctx.put("test", "foo"))
		                        .log()
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test") + "" + d)
		                        .skip(3)
		                        .take(3)
		                        .contextMap(ctx -> ctx.put("test2", "bar"))
		                        .map(d -> d)
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test2") + "" + d)
		                        .log())
		            .expectNext("barfoo4")
		            .expectNext("barfoo5")
		            .expectNext("barfoo6")
		            .verifyComplete();
	}

	@Test
	public void contextGetMono() throws InterruptedException {

		StepVerifier.create(Mono.just(1)
		                        .contextMap(ctx -> ctx.put("test", "foo"))
		                        .log()
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test") + "" + d)
		                        .map(d -> d)
		                        .map(d -> d)
		                        .contextMap(ctx -> ctx.put("test2", "bar"))
		                        .map(d -> d)
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test2") + "" + d)
		                        .log())
		            .expectNext("barfoo1")
		            .verifyComplete();
	}

	@Test
	public void contextGetHideMono() throws InterruptedException {

		StepVerifier.create(Mono.just(1)
		                        .hide()
		                        .contextMap(ctx -> ctx.put("test", "foo"))
		                        .log()
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test") + "" + d)
		                        .contextMap(ctx -> ctx.put("test2", "bar"))
		                        .map(d -> d)
		                        .map(d -> d)
		                        .contextGet((d, c) -> c.get("test2") + "" + d)
		                        .log())
		            .expectNext("barfoo1")
		            .verifyComplete();
	}
}
