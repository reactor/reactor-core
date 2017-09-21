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

package reactor.util.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierDefault;

/**
 * Leverage JSR 305 meta-annotations to define that parameters and return values are
 * non-nullable by default.
 *
 * Should be used at package level in association with {@link javax.annotation.Nullable}
 * parameters and return values level annotations.
 *
 * Present in Spring Framework 5
 *
 * @author Sebastien Deleuze
 * @since 3.1.0
 * @see javax.annotation.Nullable
 * @see javax.annotation.Nonnull
 */
//this is similar to what in done in Spring Framework 5 `org.springframework.lang.NonNullApi`
@Documented
@Nonnull
@Target({ElementType.PACKAGE, ElementType.TYPE})
@TypeQualifierDefault({ElementType.METHOD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
public @interface NonNullApi {

}
