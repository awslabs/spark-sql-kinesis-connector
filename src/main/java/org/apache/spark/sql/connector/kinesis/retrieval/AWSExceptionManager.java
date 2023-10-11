/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connector.kinesis.retrieval;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;


// This is class is based on the same class in KCL 2.5.0

/**
 * Traverses a {@code Throwable} class inheritance in search of a mapping
 * function which will convert that throwable into a {@code RuntimeException}.
 * If no mapping function is found, the default function will be applied.
 */
public class AWSExceptionManager {
  private final Map<Class<? extends Throwable>, Function<? extends Throwable, RuntimeException>>
      map = new HashMap<>();

  private Function<Throwable, RuntimeException> defaultFunction = RuntimeException::new;

  public <T extends Throwable> void add(final Class<T> clazz,
                                        final Function<T, RuntimeException> function) {
    Objects.requireNonNull(clazz);
    Objects.requireNonNull(function);
    map.put(clazz, function);
  }

  @SuppressWarnings("unchecked")
  private Function<? extends Throwable, RuntimeException> handleFor(final Throwable t) {
    Objects.requireNonNull(t);
    Class<? extends Throwable> clazz = t.getClass();
    Optional<Function<? extends Throwable, RuntimeException>> toApply = Optional.ofNullable(map.get(clazz));
    while (!toApply.isPresent() && clazz.getSuperclass() != null) {
      clazz = (Class<? extends Throwable>) clazz.getSuperclass();
      toApply = Optional.ofNullable(map.get(clazz));
    }

    return toApply.orElse(defaultFunction);
  }

  @SuppressWarnings("unchecked")
  public RuntimeException apply(Throwable t) {
    //
    // We know this is safe as the handler guarantees that the function we get will be able to accept the actual
    // type of the throwable.  handlerFor walks up the inheritance chain so we can't get a function more specific
    // than the actual type of the throwable only.
    //
    Function<Throwable, ? extends RuntimeException> f =
        (Function<Throwable, ? extends RuntimeException>) handleFor(t);
    return f.apply(t);
  }

}

