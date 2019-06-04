/*
 * Copyright (c) 2019-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tools.agent;

import java.util.function.Supplier;

/**
 * This is a helper class to return a static string supplier.
 */
@SuppressWarnings("unused")
public class ConstantSupplier implements Supplier<String> {

    public static ConstantSupplier of(String constant) {
        return new ConstantSupplier(constant);
    }

    private final String constant;

    private ConstantSupplier(String constant) {
        this.constant = constant;
    }

    @Override
    public String get() {
        return constant;
    }
}
