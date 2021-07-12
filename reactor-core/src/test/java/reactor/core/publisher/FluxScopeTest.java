/*
 * Copyright (c) 2011-Present VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.core.publisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import reactor.util.context.Context;
import reactor.util.context.ContextView;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
public class FluxScopeTest {

	//TODO perform tests around non-reactor publishers/subscribers in the middle of the chain

	@Test
	public void smokeTest() {
		Context downmost = Context.of("key1", "value1");
		List<Object> discarded = new ArrayList<>();
		List<Integer> discardedInts = new ArrayList<>();

		Flux.range(1, 20)
		    .hide()
		    .filter(i -> i % 2 == 0)
		    .scope(spec -> spec.addDiscardHandler(discarded::add)
		                       .addDiscardHandler(Integer.class, discardedInts::add),
				    (scopeCtx, f) -> f.filter(i -> i % 5 != 0))
		    .log()
		    .subscriberContext(downmost)
		    .blockLast();

		assertThat(discarded).containsExactly(10, 20);
		assertThat(discardedInts).containsExactly(10, 20);
	}

	@Test
	public void restoreByResetReturnsToOriginal() {
		AtomicReference<ContextView> contextUpstreamRef = new AtomicReference<>();
		AtomicReference<ContextView> contextDownstreamRef = new AtomicReference<>();

		Flux.range(1, 20)
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    contextUpstreamRef.set(s.getContext());
			    }
		    })
		    .hide()
		    .filter(i -> i % 2 == 0)
		    .scope(spec -> spec.setRestoreStrategyToReset()
		                       .putInContext("key1", "temp")
		                       .putInContext("key2", "inner2"),
				    (scopeCtx, f) -> f.filter(i -> i % 5 != 0))
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    contextDownstreamRef.set(s.getContext());
			    }
		    })
		    //NB: the Context provided below and the actual context (captured above) are different
		    .subscriberContext(Context.of("key1", "value1"))
		    .blockLast();

		assertThat(contextUpstreamRef.get())
				.isNotNull()
				.isSameAs(contextDownstreamRef.get())
				.matches(c -> c.size() == 1, "size(1)")
				.matches(c -> c.getOrDefault("key1", "NOT SET").equals("value1"), "key1 back to value1");
	}

	@Test
	public void restoreByRemovingAllKeysDiscardsOverwrittenKeysAndHooks() {
		AtomicReference<ContextView> contextUpstreamRef = new AtomicReference<>();

		Flux.range(1, 20)
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    contextUpstreamRef.set(s.getContext());
			    }
		    })
		    .hide()
		    .filter(i -> i % 2 == 0)
		    .scope(spec -> spec.setRestoreStrategyToRemoveAllModifiedKeys()
		                       .putInContext("key1", "temp")
		                       .putInContext("key2", "inner2"),
				    (scopeCtx, f) -> f.filter(i -> i % 5 != 0))
		    .subscriberContext(Context.of("key1", "value1"))
		    .blockLast();

		assertThat(contextUpstreamRef.get())
				.isNotNull()
				.matches(c -> c.size() == 0, "both key1 and key2 removed");
	}

	@Test
	public void restoreByRemovingHooksKeepsInnerWrites() {
		AtomicReference<ContextView> contextUpstreamRef = new AtomicReference<>();

		Flux.range(1, 20)
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    contextUpstreamRef.set(s.getContext());
			    }
		    })
		    .hide()
		    .filter(i -> i % 2 == 0)
		    .scope(spec -> spec.setRestoreStrategyToRemoveHooks()
		                       .putInContext("key1", "temp")
		                       .putInContext("key2", "inner2"),
				    (scopeCtx, f) -> f.filter(i -> i % 5 != 0))
		    .subscriberContext(Context.of("key1", "value1"))
		    .blockLast();

		assertThat(contextUpstreamRef.get())
				.isNotNull()
				.matches(c -> c.size() == 2, "size(2)")
				.matches(c -> c.getOrDefault("key1", "NOT SET").equals("temp"), "key1=temp")
				.matches(c -> c.getOrDefault("key2", "NOT SET").equals("inner2"), "key2=inner2");
	}

	@Test
	public void restoreByRemovingHooksRemovesDiscardKeyEntirely() {
		Context ctx1 = Context.of("key1", "value1");
		Consumer<Object> consumer1 = s -> {};
		Consumer<Object> consumer2 = s -> {};

		AtomicReference<ContextView> contextUpstreamRef = new AtomicReference<>();
		AtomicReference<ContextView> contextInnerRef = new AtomicReference<>();

		Flux.range(1, 20)
		    .doOnEach(s -> {
			    if (s.isOnComplete()) {
				    contextUpstreamRef.set(s.getContext());
			    }
		    })
		    .hide()
		    .filter(i -> i % 2 == 0)
		    .scope(spec -> spec.addDiscardHandler(consumer1)
		                       .setRestoreStrategyToRemoveHooks(),
				    (scopeCtx, f) -> f.doOnEach(s -> {
					    if (s.isOnComplete()) {
						    contextInnerRef.set(s.getContext());
					    }
				    })
				                     .filter(i -> i % 5 != 0))
		    .subscriberContext(c -> Operators.addOnDiscard(c, consumer2))
		    .subscriberContext(ctx1)
		    .blockLast();

		assertThat(contextInnerRef.get())
				.isNotNull()
				.matches(c -> c.size() == 2, "size(2)")
				.matches(c -> c.getOrDefault("key1", "NOT SET").equals("value1"), "key1=value1")
				.matches(c -> c.get(Hooks.KEY_ON_DISCARD) == consumer1, "inner has discard hook from options");

		assertThat(contextUpstreamRef.get())
				.isNotNull()
				.matches(c -> c.size() == 1, "size(1)")
				.matches(c -> c.getOrDefault("key1", "NOT SET").equals("value1"), "key1=value1");
	}

	@Test
	public void resetUpstreamDoesStillTriggerOuterDiscardHook() {
		List<Object> discardedOuter = new ArrayList<>();
		List<Object> discardedInner = new ArrayList<>();

		Flux.range(1, 20)
		    .hide()
		    .filter(i -> i % 5 != 0)
		    .scope(spec -> spec.addDiscardHandler(discardedInner::add)
		                       .setRestoreStrategyToReset(),
				    (scopeCtx, f) -> f)
		    .doOnDiscard(Integer.class, discardedOuter::add)
		    .blockLast();

		assertThat(discardedInner).isEmpty();
		assertThat(discardedOuter).containsExactly(5, 10, 15, 20);
	}

	@Test
	public void performContextLoggingInMiddleOfChain() {
		List<String> messages = new ArrayList<>();
		Flux<Integer> flux = Flux
				.range(1, 10)
				.scope(
						spec -> spec.putInContext("traceId", "EXAMPLE2"),
						(scopeCtx, f) -> {
							System.out.println("Original: " + scopeCtx.contextOutsideScope());
							System.out.println("Scoped: " + scopeCtx.contextInScope());

							return Flux.deferWithContext(ctx -> {
								System.out.println("Defer inside scope:" + ctx);
								String traceId = ctx.getOrDefault("traceId", "NO_SCOPED_TRACEID");
								String oldTraceId = scopeCtx.contextOutsideScope().getOrDefault("traceId", "NO_ORIGINAL_TRACEID");
								return f.doFirst(() -> messages.add("first " + traceId + " (formerly " + oldTraceId + ")"))
								        .doOnNext(v -> {
									        //there is no access to the contexts here, just the ContextView which only allow read operations
									        messages.add("onNext(" + v + ") for " + traceId);
								        })
								        .doFinally(st -> messages.add(traceId + " (formerly " + oldTraceId + ") terminated with " + st));
							});
						})
				.flatMap(i -> Flux.range(i * 10, i));

		flux.subscriberContext(Context.of("traceId", "EXAMPLE1"))
		    .blockLast();

		assertThat(messages).containsExactly("first EXAMPLE2 (formerly EXAMPLE1)",
				"onNext(1) for EXAMPLE2", "onNext(2) for EXAMPLE2",
				"onNext(3) for EXAMPLE2", "onNext(4) for EXAMPLE2",
				"onNext(5) for EXAMPLE2", "onNext(6) for EXAMPLE2",
				"onNext(7) for EXAMPLE2", "onNext(8) for EXAMPLE2",
				"onNext(9) for EXAMPLE2", "onNext(10) for EXAMPLE2",
				"EXAMPLE2 (formerly EXAMPLE1) terminated with onComplete");

		messages.clear();
		flux.blockLast();

		assertThat(messages).containsExactly("first EXAMPLE2 (formerly NO_ORIGINAL_TRACEID)",
				"onNext(1) for EXAMPLE2", "onNext(2) for EXAMPLE2",
				"onNext(3) for EXAMPLE2", "onNext(4) for EXAMPLE2",
				"onNext(5) for EXAMPLE2", "onNext(6) for EXAMPLE2",
				"onNext(7) for EXAMPLE2", "onNext(8) for EXAMPLE2",
				"onNext(9) for EXAMPLE2", "onNext(10) for EXAMPLE2",
				"EXAMPLE2 (formerly NO_ORIGINAL_TRACEID) terminated with onComplete");
	}

}
