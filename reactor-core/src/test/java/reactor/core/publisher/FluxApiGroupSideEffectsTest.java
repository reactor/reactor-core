/*
 * Copyright (c) 2021 VMware Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
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

import org.junit.jupiter.params.provider.ValueSource;

import reactor.core.Scannable;
import reactor.test.ParameterizedTestWithName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Simon Baslé
 */
class FluxApiGroupSideEffectsTest {

	@ParameterizedTestWithName
	@ValueSource(booleans = { false, true })
	void cumulativePatternReordersEvents(boolean fuseable) {
		List<String> events = new ArrayList<>();
		Flux<Integer> source = Flux.range(1, 10);
		if (!fuseable) {
			source = source.hide();
		}

		Flux<Integer> withSideEffects = source.sideEffects(spec -> spec
				.doFirst(() -> events.add("first A"))
				.doOnNext(n -> events.add("onNext(" + n + ")"))
				.doOnCancel(() -> events.add("cancelled"))
				.doOnNext(n -> events.add("\tB onNext(" + n + ")"))
				.doOnRequest(r -> events.add("onRequest(" + r + ")"))
				.doFinally(sigType -> events.add("terminated with " + sigType))
				.doOnRequest(r -> events.add("\tB onRequest(" + r + ")"))
				.doOnCancel(() -> events.add("\tcancelled B"))
				.doFirst(() -> events.add("\tfirst B"))
				.doFinally(sigType -> events.add("\tterminated B with " + sigType))
			);

		withSideEffects
			.take(10, true)
			.take(3, false)
			.blockLast();

		assertThat(events).containsExactly(
			"first A",
			"	first B",
			"onRequest(10)",
			"	B onRequest(10)",
			"onNext(1)",
			"	B onNext(1)",
			"onNext(2)",
			"	B onNext(2)",
			"onNext(3)",
			"	B onNext(3)",
			"cancelled",
			"	cancelled B",
			"terminated with cancel",
			"	terminated B with cancel"
		);

		assertThat(Scannable.from(withSideEffects).steps())
			.as("macro fused into 3 operators")
			.hasSize(fuseable ? 4 : 5)
			.endsWith("doFirst", "peek", "doFinally");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = { false, true })
	void specOnError(boolean fuseable) {
		List<String> events = new ArrayList<>();
		Flux<Integer> source = Flux
			.range(1, 10)
			.map(i -> 100 / (10 - i));
		if (!fuseable) {
			source = source.hide();
		}

		Flux<Integer> withSideEffects = source.sideEffects(spec -> spec
				.doFirst(() -> events.add("first"))
				.doFinally(sigType -> events.add("terminated with " + sigType))
				.doOnError(error -> events.add("onError"))
				.doAfterTerminate(() -> events.add("afterErrorOrComplete"))
				.doAfterTerminate(() -> events.add("\tafterErrorOrComplete B"))
				.doOnError(error -> events.add("\tonError B"))
			);

		withSideEffects.onErrorReturn(-1).blockLast(); //we ignore the expected ArithmeticException

		assertThat(events).containsExactly(
			"first",
			"onError",
			"	onError B",
			"terminated with onError",
			"afterErrorOrComplete",
			"	afterErrorOrComplete B"
		);

		assertThat(Scannable.from(withSideEffects).steps())
			.as("macro fused into 3 operators")
			.hasSize(fuseable ? 5 : 6)
			.endsWith("doFirst", "peek", "doFinally");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = { false, true })
	void specOnComplete(boolean fuseable) {
		List<String> events = new ArrayList<>();
		Flux<Integer> source = Flux.range(1, 10);
		if (!fuseable) {
			source = source.hide();
		}

		Flux<Integer> withSideEffects = source.sideEffects(spec -> spec
				.doFirst(() -> events.add("first"))
				.doFinally(sigType -> events.add("terminated with " + sigType))
				.doOnComplete(() -> events.add("onComplete"))
				.doAfterTerminate(() -> events.add("afterErrorOrComplete"))
				.doAfterTerminate(() -> events.add("\tafterErrorOrComplete B"))
				.doOnComplete(() -> events.add("\tonComplete B"))
			);

		withSideEffects.blockLast();

		assertThat(events).containsExactly(
			"first",
			"onComplete",
			"	onComplete B",
			"terminated with onComplete",
			"afterErrorOrComplete",
			"	afterErrorOrComplete B"
		);

		assertThat(Scannable.from(withSideEffects).steps())
			.as("macro fused into 3 operators")
			.hasSize(fuseable ? 4 : 5)
			.endsWith("doFirst", "peek", "doFinally");
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = { false, true })
	void oneSpecPerOperatorHasCounterintuitiveOrder(boolean fuseable) {
		List<String> events = new ArrayList<>();
		Flux<Integer> source = Flux.range(1, 10);
		if (!fuseable) {
			source = source.hide();
		}

		Flux<Integer> withSideEffects = source
			.sideEffects(spec -> spec.doFirst(() -> events.add("first A")))
			.sideEffects(spec -> spec.doOnNext(n -> events.add("onNext(" + n + ")")))
			.sideEffects(spec -> spec.doOnCancel(() -> events.add("cancelled")))
			.sideEffects(spec -> spec.doOnNext(n -> events.add("\tB onNext(" + n + ")")))
			.sideEffects(spec -> spec.doOnRequest(r -> events.add("onRequest(" + r + ")")))
			.sideEffects(spec -> spec.doFinally(sigType -> events.add("terminated with " + sigType)))
			.sideEffects(spec -> spec.doOnRequest(r -> events.add("\tB onRequest(" + r + ")")))
			.sideEffects(spec -> spec.doOnCancel(() -> events.add("\tcancelled B")))
			.sideEffects(spec -> spec.doFirst(() -> events.add("\tfirst B")))
			.sideEffects(spec -> spec.doFinally(sigType -> events.add("\tterminated B with " + sigType)));

		withSideEffects
			.take(10, true)
			.take(3, false)
			.blockLast();

		String[] expected = new String[] {
			"first A",
			"	first B",
			"onRequest(10)",
			"	B onRequest(10)",
			"onNext(1)",
			"	B onNext(1)",
			"onNext(2)",
			"	B onNext(2)",
			"onNext(3)",
			"	B onNext(3)",
			"cancelled",
			"	cancelled B",
			"terminated with cancel",
			"	terminated B with cancel"
		};

		assertThat(events)
			.as("not careful with order gives a different order")
			.doesNotContainSequence(expected)
			.containsExactlyInAnyOrder(expected);

		assertThat(Scannable.from(withSideEffects).steps())
			.as("as many operators, in declared order")
			.hasSize(fuseable ? 11 : 12)
			.endsWith(
				"doFirst",
				"peek",
				"peek",
				"peek",
				"peek",
				"doFinally",
				"peek",
				"peek",
				"doFirst",
				"doFinally"
			);
	}

	@ParameterizedTestWithName
	@ValueSource(booleans = { false, true })
	void specOnSubscriptionReceivedVsFirst(boolean fuseable) {
		List<String> events = new ArrayList<>();
		Flux<Integer> source = Flux.range(1, 10);
		if (!fuseable) {
			source = source.hide();
		}

		Flux<Integer> withSideEffects = source.sideEffects(spec -> spec
			.doOnSubscriptionReceived(sub -> events.add("onSubscribe(Subscription)"))
			.doOnSubscriptionReceived(sub -> events.add("\tonSubscribe(Subscription) B"))
			.doFirst(() -> events.add("first"))
		);

		withSideEffects.blockLast();

		assertThat(events)
			.as("first then onSubscribe as declared")
			.containsExactly(
				"first",
				"onSubscribe(Subscription)",
				"\tonSubscribe(Subscription) B"
			);

		assertThat(Scannable.from(withSideEffects).steps())
			.as("macro fused into 2 operators")
			.hasSize(fuseable ? 3 : 4)
			.endsWith(
				"doFirst",
				"peek"
			);
	}
}