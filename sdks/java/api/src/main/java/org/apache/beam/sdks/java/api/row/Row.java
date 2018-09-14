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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdks.java.api.row.Schema.TypeName;

/**
 * {@link Row} is an immutable tuple-like schema to represent one element in a PCollection. The
 * fields are described with a {@link Schema}.
 *
 * <p>{@link Schema} contains the names for each field and the coder for the whole record,
 * {see @link Schema#getRowCoder()}.
 */
// @Experimental
public interface Row extends Serializable {
  /** @return the schema associated to this row. */
  Schema getSchema();

  /**
   * Unique needed primitive to access the data. All others are derived from this one.
   *
   * @param fieldIdx the field index in the schema.
   * @param <T> the expected type of the value.
   * @return the value extracted from the internal storage of the row.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <T> T getValue(int fieldIdx);

  /**
   * Extract one value of this row.
   *
   * @param fieldName the field name to extract.
   * @param <T> the expected type.
   * @return the value.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  default <T> T getValue(final String fieldName) {
    return getValue(getSchema().indexOf(fieldName));
  }

  /** @return all values of this row, sorted by index. */
  default List<Object> getValues() {
    return getSchema().getFields().stream().map(f -> getValue(f.getName())).collect(toList());
  }

  /**
   * Get a {@link TypeName#BYTE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Byte getByte(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#BYTE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Byte getByte(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#BYTES} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default byte[] getBytes(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#BYTES} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default byte[] getBytes(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#INT16} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Short getInt16(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#INT16} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Short getInt16(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#INT32} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Integer getInt32(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#INT32} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Integer getInt32(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#INT64} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Long getInt64(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#INT64} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Long getInt64(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#DECIMAL} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default BigDecimal getDecimal(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#DECIMAL} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default BigDecimal getDecimal(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Float getFloat(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Float getFloat(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Double getDouble(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Double getDouble(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#STRING} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default String getString(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#STRING} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default String getString(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#BOOLEAN} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Boolean getBoolean(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#BOOLEAN} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Boolean getBoolean(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field index, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default ZonedDateTime getDateTime(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default ZonedDateTime getDateTime(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#ROW} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @return the extracted value from the row.
   */
  default Row getRow(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#ROW} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @return the extracted value from the row.
   */
  default Row getRow(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#ARRAY} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @param <T> type of the items of the list.
   * @return the extracted value from the row.
   */
  default <T> List<T> getArray(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#ARRAY} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @param <T> type of the items of the list.
   * @return the extracted value from the row.
   */
  default <T> List<T> getArray(final String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link TypeName#MAP} value by field index, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldIndex the index of the field to extract.
   * @param <K> type of the keys of the map.
   * @param <V> type of the values of the map.
   * @return the extracted value from the row.
   */
  default <K, V> Map<K, V> getMap(final int fieldIndex) {
    return getValue(fieldIndex);
  }

  /**
   * Get a {@link TypeName#MAP} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   *
   * @param fieldName the name of the field to extract.
   * @param <K> type of the keys of the map.
   * @param <V> type of the values of the map.
   * @return the extracted value from the row.
   */
  default <K, V> Map<K, V> getMap(final String fieldName) {
    return getValue(fieldName);
  }

  /** Builder for {@link Row}. */
  interface Builder {
    /**
     * Set a value at a particular index, it must match schema definition.
     *
     * @param index the field index.
     * @param value the value to set.
     * @param <T> the value type.
     * @return this builder.
     */
    <T> Builder withValue(int index, T value);

    /**
     * Set a value at a particular index, it must match schema definition.
     *
     * @param name the field name.
     * @param value the value to set.
     * @param <T> the value type.
     * @return this builder.
     */
    <T> Builder withValue(String name, T value);

    default Builder withValues(final List<Object> values) {
      IntStream.range(0, values.size()).forEach(i -> withValue(i, values.get(i)));
      return this;
    }

    Row build();
  }

  /**
   * Creates a new record filled with nulls.
   *
   * @param factory the factory to create the row builder.
   * @param schema the row schema.
   * @return the row built with null values.
   */
  static Row nullRow(final BuilderFactory factory, final Schema schema) {
    return factory
        .newRowBuilder(schema)
        .withValues(Collections.nCopies(schema.getFields().size(), null))
        .build();
  }
}
