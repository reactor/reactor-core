/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.fn;

/**
 * Implementations of this class perform work on the given parameters pair.
 *
 * @param <LEFT>  The type of the left value to the apply operation
 * @param <RIGHT> The type of the right to the apply operation
 * @author Stephane Maldini
 */
public interface BiConsumer<LEFT, RIGHT> {

	void accept(LEFT left, RIGHT right);
}
