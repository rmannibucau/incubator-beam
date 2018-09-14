/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
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
package org.apache.beam.sdks.java.api.row;

/**
 * Entry point to plug custom implementations of Row and Schema. This allows for instance
 * implementation to only manipulate Avro, Protobuf or anything else and not have any custom
 * serialization.
 *
 * <p>This factory is assumed loaded only once then the implementation is stored. In Beam it is
 * typically loaded by the Pipeline then accessible through pipeline.getBuilderFactory().
 *
 * <p>Loading logic can look like:
 *
 * <pre><code>
 * static BuilderFactory load() {
 *     final Iterator&lt;BuilderFactory&gt; iterator = ServiceLoader.load(BuilderFactory.class,
 *             ofNullable(BuilderFactory.class.getClassLoader()).orElseGet(ClassLoader::getSystemClassLoader))
 *             .iterator();
 *     if (iterator.hasNext()) {
 *         final BuilderFactory impl = iterator.next();
 *         if (iterator.hasNext()) {
 *             throw new IllegalStateException("Ambiguous BuilderFactory implementation: " + asList(impl,
 *             iterator.next()));
 *         }
 *         return impl;
 *     }
 *     throw new IllegalStateException("No BuilderFactory implementation found");
 * }
 * </code></pre>
 */
public interface BuilderFactory {

  /**
   * Creates a row builder.
   *
   * @param schema the row schema.
   * @return a new row builder.
   */
  Row.Builder newRowBuilder(Schema schema);

  /**
   * Creates a schema builder.
   *
   * @return a new schema builder.
   */
  Schema.Builder newSchemaBuilder();

  /**
   * Creates a field type builder.
   *
   * @param type the type of the field type being built.
   * @return a new field type builder.
   */
  Schema.FieldType.Builder newFieldTypeBuilder(Schema.TypeName type);
}
