/*
 * Copyright (c) 2022 VMware Inc. or its affiliates, All Rights Reserved.
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

package reactor.util.function;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Simon Baslé
 */
public interface FunctionalWrappers {

	/*
	As of 2022, java.util.function.* classes imported by Reactor are:
		x java.util.function.BiConsumer
		x java.util.function.BiFunction
		java.util.function.BinaryOperator (not used in public API)
		x java.util.function.BiPredicate
		x java.util.function.BooleanSupplier (used in Flux#repeat)
		x java.util.function.Consumer
		x java.util.function.Function
		java.util.function.IntFunction (not used in public API)
		x java.util.function.LongConsumer (used in Mono API)
		x java.util.function.Predicate
		x java.util.function.Supplier
	 */

	Runnable runnable(Runnable original);

	<T> Callable<T> callable(Callable<T> original);

	<T> Consumer<T> consumer(Consumer<T> original);

	<T, U> BiConsumer<T, U> biConsumer(BiConsumer<T, U> original);

	<T> Supplier<T> supplier(Supplier<T> original);

	<T, R> Function<T, R> function(Function<T, R> original);

	<T1, T2, R> BiFunction<T1, T2, R> biFunction(BiFunction<T1, T2, R> original);

	<T> Predicate<T> predicate(Predicate<T> original);

	<T, U> BiPredicate<T, U> biPredicate(BiPredicate<T, U> original);

	BooleanSupplier booleanSupplier(BooleanSupplier original);

	LongConsumer longConsumer(LongConsumer original);

	/**
	 * A constant instance of {@link FunctionalWrappers} that provides identity wrapping:
	 * each method returns the original, making the "wrapping" no-op.
	 */
	FunctionalWrappers IDENTITY = new FunctionalWrappers() {
		@Override
		public Runnable runnable(Runnable original) {
			return original;
		}

		@Override
		public <T> Callable<T> callable(Callable<T> original) {
			return original;
		}

		@Override
		public <T> Consumer<T> consumer(Consumer<T> original) {
			return original;
		}

		@Override
		public <T, U> BiConsumer<T, U> biConsumer(BiConsumer<T, U> original) {
			return original;
		}

		@Override
		public <T> Supplier<T> supplier(Supplier<T> original) {
			return original;
		}

		@Override
		public <T, R> Function<T, R> function(Function<T, R> original) {
			return original;
		}

		@Override
		public <T1, T2, R> BiFunction<T1, T2, R> biFunction(BiFunction<T1, T2, R> original) {
			return original;
		}

		@Override
		public <T> Predicate<T> predicate(Predicate<T> original) {
			return original;
		}

		@Override
		public <T, U> BiPredicate<T, U> biPredicate(BiPredicate<T, U> original) {
			return original;
		}

		@Override
		public BooleanSupplier booleanSupplier(BooleanSupplier original) {
			return original;
		}

		@Override
		public LongConsumer longConsumer(LongConsumer original) {
			return original;
		}
	};


}
