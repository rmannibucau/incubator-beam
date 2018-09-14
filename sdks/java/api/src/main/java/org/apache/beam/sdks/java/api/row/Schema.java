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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/** {@link Schema} describes the fields in {@link Row}. */
public interface Schema extends Serializable {
  /** Builder class for building {@link Schema} objects. */
  interface Builder {
    // todo: needed?
    default Builder addFields(final List<Field> fields) {
      fields.forEach(this::addField);
      return this;
    }

    /**
     * @param field field to include in the schema.
     * @return this builder.
     */
    Builder addField(Field field);

    /** @return the described schema built. */
    Schema build();
  }

  List<Field> getFields();

  /**
   * An enumerated list of type constructors.
   *
   * <ul>
   *   <li>Atomic types are built from type constructors that take no arguments
   *   <li>Arrays, rows, and maps are type constructors that take additional arguments to form a
   *       valid {@link FieldType}.
   * </ul>
   */
  enum TypeName {
    BYTE, // One-byte signed integer.
    INT16, // two-byte signed integer. (short)
    INT32, // four-byte signed integer. (int)
    INT64, // eight-byte signed integer. (long)
    DECIMAL, // Decimal integer
    FLOAT,
    DOUBLE,
    STRING, // String.
    DATETIME, // Date and time. (ZonedDateTime)
    BOOLEAN, // Boolean.
    BYTES, // Byte array.
    ARRAY,
    MAP,
    ROW // The field is itself a nested row.
  }

  /**
   * A descriptor of a single field type. This is a recursive descriptor, as nested types are
   * allowed.
   */
  interface FieldType extends Serializable {
    // Returns the type of this field.
    TypeName getTypeName();

    // For container types (e.g. ARRAY), returns the type of the contained element.
    FieldType getCollectionElementType();

    // For MAP type, returns the type of the key element, it must be a primitive type;
    FieldType getMapKeyType();

    // For MAP type, returns the type of the value element, it can be a nested type;
    FieldType getMapValueType();

    // For ROW types, returns the schema for the row.
    Schema getRowSchema();

    // Returns optional extra metadata. TBD: change byte[]?
    byte[] getMetadata();

    interface Builder {
      Schema.FieldType.Builder withTypeName(Schema.TypeName typeName);

      Schema.FieldType.Builder withCollectionElementType(Schema.FieldType collectionElementType);

      Schema.FieldType.Builder withMapKeyType(Schema.FieldType mapKeyType);

      Schema.FieldType.Builder withMapValueType(Schema.FieldType mapValueType);

      Schema.FieldType.Builder withRowSchema(Schema rowSchema);

      Schema.FieldType.Builder withMetadata(byte[] metadata);

      Schema.FieldType build();
    }
  }

  /** Field of a row. Contains the {@link FieldType} along with associated metadata. */
  interface Field extends Serializable {
    /**
     * Returns the field name.
     *
     * @return field name.
     */
    String getName();

    /**
     * Returns the field's description.
     *
     * @return field description.
     */
    String getDescription();

    /**
     * Returns the fields {@link FieldType}.
     *
     * @return field type.
     */
    Schema.FieldType getType();

    /**
     * Returns whether the field supports null values.
     *
     * @return the field nullability.
     */
    boolean getNullable();

    interface Builder {
      Schema.Field.Builder withName(String name);

      Schema.Field.Builder withDescription(String description);

      Schema.Field.Builder withType(Schema.FieldType type);

      Schema.Field.Builder withNullable(Boolean nullable);

      Schema.Field build();
    }
  }

  /**
   * Return the list of all field names.
   *
   * @return the field names.
   */
  default List<String> getFieldNames() {
    return getFields().stream().map(Schema.Field::getName).collect(Collectors.toList());
  }

  /**
   * Return a field by index.
   *
   * @param index the index of the field to extract.
   * @return the extracted field.
   */
  default Field getField(final int index) {
    return getFields().get(index);
  }

  /**
   * Extract a field from this schema from its name.
   *
   * @param name name of the field.
   * @return the extracted field.
   */
  default Field getField(final String name) {
    return getFields().get(indexOf(name));
  }

  /**
   * Find the index of a given field.
   *
   * @param fieldName the field name to filter the fields.
   * @return the index of the field or -1.
   */
  default int indexOf(final String fieldName) {
    final Field field =
        getFields().stream().filter(it -> it.getName().equals(fieldName)).findFirst().orElse(null);
    // respect indexOf semantic
    return field == null ? -1 : getFields().indexOf(field);
  }

  /**
   * Returns true if {@code fieldName} exists in the schema, false otherwise.
   *
   * @param fieldName the field name to check the existence to.
   * @return true if the field exists, false otherwise.
   */
  default boolean contains(final String fieldName) {
    return indexOf(fieldName) >= 0;
  }

  /**
   * Return the name of field by index.
   *
   * @param fieldIndex the field index to extract.
   * @return the field name.
   */
  default String nameOf(final int fieldIndex) {
    final Field field =
        requireNonNull(getFields().get(fieldIndex), "Missing field at index " + fieldIndex);
    return field.getName();
  }
}
