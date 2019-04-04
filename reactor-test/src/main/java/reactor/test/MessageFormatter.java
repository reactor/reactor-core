/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
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

package reactor.test;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

import reactor.util.annotation.Nullable;

/**
 * A utility class to format error messages in all of the default implementation of {@link StepVerifier}.
 * Formats events and is capable of prepending messages with a {@link StepVerifierOptions#scenarioName(String) scenario name}.
 *
 * @author Simon Baslé
 */
final class MessageFormatter {

	private static final String                            EMPTY = "";

	final String scenarioPrefix;
	@Nullable
	final ValueFormatters.ToStringConverter valueFormatter;
	@Nullable
	final Collection<ValueFormatters.Extractor<?>> extractors;

	MessageFormatter(@Nullable final String scenarioName,
			@Nullable ValueFormatters.ToStringConverter valueFormatter,
			@Nullable Collection<ValueFormatters.Extractor<?>> extractors) {
		if (scenarioName == null || scenarioName.isEmpty()) {
			scenarioPrefix = EMPTY;
		}
		else {
			scenarioPrefix = "[" + scenarioName + "] ";
		}
		this.valueFormatter =  valueFormatter;
		this.extractors = extractors;
	}

	/**
	 * Format an event, message with argument placeholders (in the style of {@link String#format(String, Object...)})
	 * and produce an {@link AssertionError}.
	 *
	 * @param event the event that caused an expectation failure
	 * @param msg the message, possibly with {@link String#format(String, Object...)} style placeholders
	 * @param args the optional values for the placeholders in msg
	 * @return an {@link AssertionError} with a standardized message potentially prefixed with the associated scenario name
	 */
	AssertionError fail(@Nullable DefaultStepVerifierBuilder.Event<?> event, String msg, Object... args) {
		String prefix;
		if (event != null && event.getDescription()
		                          .length() > 0) {
			prefix = String.format("expectation \"%s\" failed (", event.getDescription());
		}
		else {
			prefix = "expectation failed (";
		}

		return failPrefix(prefix, msg, args);
	}

	/**
	 * Format an event, message with argument placeholders (in the style of {@link String#format(String, Object...)})
	 * and produce an {@link AssertionError} wrapped in an {@link Optional}.
	 *
	 * @param event the event that caused an expectation failure
	 * @param msg the message, possibly with {@link String#format(String, Object...)} style placeholders
	 * @param args the optional values for the placeholders in msg
	 * @return an {@link AssertionError} with a standardized message potentially prefixed with the associated scenario name,
	 * wrapped in an {@link Optional}
	 */
	Optional<AssertionError> failOptional(@Nullable DefaultStepVerifierBuilder.Event<?> event, String msg,
			Object... args) {
		return Optional.of(fail(event, msg, args));
	}

	/**
	 * Formats a two-part message: a prefix that should end in an opening parenthesis and
	 * a second part with argument placeholders (in the style of {@link String#format(String, Object...)}).
	 * This method adds the closing parenthesis after that second part, and then produces
	 * an {@link AssertionError} out of the formatted message.
	 *
	 * @param prefix the first part of the message, should en with an opening parenthesis
	 * @param msg the second part (detail) of the message, possibly with {@link String#format(String, Object...)} style placeholders
	 * @param args the optional values for the placeholders in msg
	 * @return an {@link AssertionError} with a standardized message potentially prefixed with the associated scenario name
	 */
	AssertionError failPrefix(String prefix, String msg, Object... args) {
		String formattedMessage = format(msg, args);
		return assertionError(prefix + formattedMessage + ")");
	}

	/**
	 * Produce an {@link AssertionError} out of a given plain message, potentially prefixing
	 * it with the associated scenario name.
	 *
	 * @param msg the plain message
	 * @return an {@link AssertionError} with a standardized message potentially prefixed with the associated scenario name
	 */
	AssertionError assertionError(String msg) {
		return new AssertionError(scenarioPrefix + msg);
	}

	/**
	 * Produce an {@link AssertionError} out of a given plain message and a cause,
	 * potentially prefixing it with the associated scenario name.
	 *
	 * @param msg the plain message
	 * @param cause the cause to add to the {@link AssertionError}
	 * @return an {@link AssertionError} with a cause and a standardized message
	 * potentially prefixed with the associated scenario name
	 */
	AssertionError assertionError(String msg, @Nullable Throwable cause) {
		return new AssertionError(scenarioPrefix + msg, cause); //null cause is ok
	}

	/**
	 * Produce an arbitrary {@link Throwable} with a standardized message comprised of
	 * the given plain message and an optional prefix if the associated scenario is named.
	 *
	 * @param errorProducer the error producer (usually an Exception constructor method reference)
	 * @param message the plain message
	 * @param <T> the type of the {@link Exception}
	 * @return a {@link T} with a message potentially prefixed with the associated scenario name
	 */
	<T extends Throwable> T error(Function<String, T> errorProducer, String message) {
		return errorProducer.apply(scenarioPrefix + message);
	}

	/**
	 * Format a message with placeholders (in the {@link String#format(String, Object...)}
	 * style), with a potential customization of each argument's {@link Object#toString() toString}.
	 *
	 * @param msg the message base, with placeholders for arguments
	 * @param args the arguments
	 * @return the formatted message
	 */
	String format(String msg, Object... args) {
		if (valueFormatter != null) {
			return String.format(msg, ValueFormatters.convertVarArgs(valueFormatter, extractors, args));
		}
		return String.format(msg, args);
	}
}
