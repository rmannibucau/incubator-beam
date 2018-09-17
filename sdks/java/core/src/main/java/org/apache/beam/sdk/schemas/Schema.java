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
package org.apache.beam.sdk.schemas;

import static org.apache.beam.sdks.java.api.row.Schema.TypeName.ARRAY;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.BYTE;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.DATETIME;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.DECIMAL;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.DOUBLE;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.FLOAT;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.INT16;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.INT32;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.INT64;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.MAP;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.ROW;
import static org.apache.beam.sdks.java.api.row.Schema.TypeName.STRING;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.values.Row;

/** {@link Schema} describes the fields in {@link Row}. */
@Experimental(Kind.SCHEMAS)
@SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
public class Schema implements Serializable, org.apache.beam.sdks.java.api.row.Schema {
  // A mapping between field names an indices.
  private final BiMap<String, Integer> fieldIndices = HashBiMap.create();
  private final List<org.apache.beam.sdks.java.api.row.Schema.Field> fields;
  // Cache the hashCode, so it doesn't have to be recomputed. Schema objects are immutable, so this
  // is correct.
  private final int hashCode;
  // Every SchemaCoder has a UUID. The schemas created with the same UUID are guaranteed to be
  // equal, so we can short circuit comparison.
  @Nullable private UUID uuid = null;

  /** Builder class for building {@link Schema} objects. */
  public static class Builder {
    List<Field> fields;

    public Builder() {
      this.fields = Lists.newArrayList();
    }

    public Builder addFields(List<Field> fields) {
      this.fields.addAll(fields);
      return this;
    }

    public Builder addFields(Field... fields) {
      return addFields(Arrays.asList(fields));
    }

    public Builder addField(Field field) {
      fields.add(field);
      return this;
    }

    public Builder addField(String name, FieldType type) {
      fields.add(Field.of(name, type));
      return this;
    }

    public Builder addNullableField(String name, FieldType type) {
      fields.add(Field.nullable(name, type));
      return this;
    }

    public Builder addByteField(String name) {
      fields.add(Field.of(name, FieldType.BYTE));
      return this;
    }

    public Builder addByteArrayField(String name) {
      fields.add(Field.of(name, FieldType.BYTES));
      return this;
    }

    public Builder addInt16Field(String name) {
      fields.add(Field.of(name, FieldType.INT16));
      return this;
    }

    public Builder addInt32Field(String name) {
      fields.add(Field.of(name, FieldType.INT32));
      return this;
    }

    public Builder addInt64Field(String name) {
      fields.add(Field.of(name, FieldType.INT64));
      return this;
    }

    public Builder addDecimalField(String name) {
      fields.add(Field.of(name, FieldType.DECIMAL));
      return this;
    }

    public Builder addFloatField(String name) {
      fields.add(Field.of(name, FieldType.FLOAT));
      return this;
    }

    public Builder addDoubleField(String name) {
      fields.add(Field.of(name, FieldType.DOUBLE));
      return this;
    }

    public Builder addStringField(String name) {
      fields.add(Field.of(name, FieldType.STRING));
      return this;
    }

    public Builder addDateTimeField(String name) {
      fields.add(Field.of(name, FieldType.DATETIME));
      return this;
    }

    public Builder addBooleanField(String name) {
      fields.add(Field.of(name, FieldType.BOOLEAN));
      return this;
    }

    public Builder addArrayField(String name, FieldType collectionElementType) {
      fields.add(Field.of(name, FieldType.array(collectionElementType)));
      return this;
    }

    public Builder addRowField(String name, Schema fieldSchema) {
      fields.add(Field.of(name, FieldType.row(fieldSchema)));
      return this;
    }

    public Builder addMapField(String name, FieldType keyType, FieldType valueType) {
      fields.add(Field.of(name, FieldType.map(keyType, valueType)));
      return this;
    }

    public Schema build() {
      return new Schema(fields);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public Schema(List<Field> fields) {
    this.fields = new ArrayList<>(fields);
    int index = 0;
    for (Field field : fields) {
      fieldIndices.put(field.getName(), index++);
    }
    this.hashCode = Objects.hash(fieldIndices, fields);
  }

  public static Schema of(Field... fields) {
    return Schema.builder().addFields(fields).build();
  }

  /** Set this schema's UUID. All schemas with the same UUID must be guaranteed to be identical. */
  public void setUUID(UUID uuid) {
    this.uuid = uuid;
  }

  /** Get this schema's UUID. */
  @Nullable
  public UUID getUUID() {
    return this.uuid;
  }

  /** Returns true if two Schemas have the same fields in the same order. */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema other = (Schema) o;
    // If both schemas have a UUID set, we can simply compare the UUIDs.
    if (uuid != null && other.uuid != null) {
      return Objects.equals(uuid, other.uuid);
    }
    return Objects.equals(fieldIndices, other.fieldIndices)
        && Objects.equals(getFields(), other.getFields());
  }

  enum EquivalenceNullablePolicy {
    SAME,
    WEAKEN,
    IGNORE
  };

  /** Returns true if two Schemas have the same fields, but possibly in different orders. */
  public boolean equivalent(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.SAME);
  }

  /** Returns true if this Schema can be assigned to another Schema. * */
  public boolean assignableTo(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.WEAKEN);
  }

  /** Returns true if this Schema can be assigned to another Schema, igmoring nullable. * */
  public boolean assignableToIgnoreNullable(Schema other) {
    return equivalent(other, EquivalenceNullablePolicy.IGNORE);
  }

  private boolean equivalent(Schema other, EquivalenceNullablePolicy nullablePolicy) {
    if (other.getFieldCount() != getFieldCount()) {
      return false;
    }

    List<org.apache.beam.sdks.java.api.row.Schema.Field> otherFields =
        other
            .getFields()
            .stream()
            .sorted(Comparator.comparing(org.apache.beam.sdks.java.api.row.Schema.Field::getName))
            .collect(Collectors.toList());
    List<org.apache.beam.sdks.java.api.row.Schema.Field> actualFields =
        getFields()
            .stream()
            .sorted(Comparator.comparing(org.apache.beam.sdks.java.api.row.Schema.Field::getName))
            .collect(Collectors.toList());

    for (int i = 0; i < otherFields.size(); ++i) {
      Field otherField = Field.class.cast(otherFields.get(i));
      Field actualField = Field.class.cast(actualFields.get(i));
      if (!otherField.equivalent(actualField, nullablePolicy)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Fields:\n");
    for (org.apache.beam.sdks.java.api.row.Schema.Field field : fields) {
      builder.append(field);
      builder.append("\n");
    }
    return builder.toString();
  };

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public List<org.apache.beam.sdks.java.api.row.Schema.Field> getFields() {
    return fields;
  }

  /**
   * An enumerated list of type constructors.
   *
   * <ul>
   *   <li>Atomic types are built from type constructors that take no arguments
   *   <li>Arrays, rows, and maps are type constructors that take additional arguments to form a
   *       valid {@link FieldType}.
   * </ul>
   */
  @SuppressWarnings("MutableConstantField")
  public static final class TypeNameHelper {
    private TypeNameHelper() {
      // no-op
    }

    public static final Set<TypeName> NUMERIC_TYPES =
        ImmutableSet.of(BYTE, INT16, INT32, INT64, DECIMAL, FLOAT, DOUBLE);
    public static final Set<TypeName> STRING_TYPES = ImmutableSet.of(STRING);
    public static final Set<TypeName> DATE_TYPES = ImmutableSet.of(DATETIME);
    public static final Set<TypeName> COLLECTION_TYPES = ImmutableSet.of(ARRAY);
    public static final Set<TypeName> MAP_TYPES = ImmutableSet.of(MAP);
    public static final Set<TypeName> COMPOSITE_TYPES = ImmutableSet.of(ROW);

    public static boolean isPrimitiveType(final TypeName typeName) {
      return !isCollectionType(typeName) && !isMapType(typeName) && !isCompositeType(typeName);
    }

    public static boolean isNumericType(final TypeName typeName) {
      return NUMERIC_TYPES.contains(typeName);
    }

    public static boolean isStringType(final TypeName typeName) {
      return STRING_TYPES.contains(typeName);
    }

    public static boolean isDateType(final TypeName typeName) {
      return DATE_TYPES.contains(typeName);
    }

    public static boolean isCollectionType(final TypeName typeName) {
      return COLLECTION_TYPES.contains(typeName);
    }

    public static boolean isMapType(final TypeName typeName) {
      return MAP_TYPES.contains(typeName);
    }

    public static boolean isCompositeType(final TypeName typeName) {
      return COMPOSITE_TYPES.contains(typeName);
    }
  }

  /**
   * A descriptor of a single field type. This is a recursive descriptor, as nested types are
   * allowed.
   */
  @Immutable
  @SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
  public static final class FieldType
      implements Serializable, org.apache.beam.sdks.java.api.row.Schema.FieldType {
    private final Schema.TypeName typeName;
    @Nullable private final Schema.FieldType collectionElementType;
    @Nullable private final Schema.FieldType mapKeyType;
    @Nullable private final Schema.FieldType mapValueType;
    @Nullable private final Schema rowSchema;
    @Nullable private final byte[] metadata;

    private FieldType(
        Schema.TypeName typeName,
        @Nullable Schema.FieldType collectionElementType,
        @Nullable Schema.FieldType mapKeyType,
        @Nullable Schema.FieldType mapValueType,
        @Nullable Schema rowSchema,
        @Nullable byte[] metadata) {
      this.typeName = typeName;
      this.collectionElementType = collectionElementType;
      this.mapKeyType = mapKeyType;
      this.mapValueType = mapValueType;
      this.rowSchema = rowSchema;
      this.metadata = metadata;
    }

    @Override
    public Schema.TypeName getTypeName() {
      return typeName;
    }

    @Nullable
    @Override
    public Schema.FieldType getCollectionElementType() {
      return collectionElementType;
    }

    @Nullable
    @Override
    public Schema.FieldType getMapKeyType() {
      return mapKeyType;
    }

    @Nullable
    @Override
    public Schema.FieldType getMapValueType() {
      return mapValueType;
    }

    @Nullable
    @Override
    public Schema getRowSchema() {
      return rowSchema;
    }

    @SuppressWarnings(value = {"mutable"})
    @Nullable
    @Override
    public byte[] getMetadata() {
      return metadata;
    }

    @Override
    public String toString() {
      return "FieldType{"
          + "typeName="
          + typeName
          + ", "
          + "collectionElementType="
          + collectionElementType
          + ", "
          + "mapKeyType="
          + mapKeyType
          + ", "
          + "mapValueType="
          + mapValueType
          + ", "
          + "rowSchema="
          + rowSchema
          + ", "
          + "metadata="
          + Arrays.toString(metadata)
          + "}";
    }

    public Schema.FieldType.Builder toBuilder() {
      return new Builder(this);
    }

    public static FieldType.Builder forTypeName(TypeName typeName) {
      return new Builder().setTypeName(typeName);
    }

    /** Builder for a {@link Schema.FieldType}. */
    public static final class Builder {
      @Nullable private Schema.TypeName typeName;
      @Nullable private Schema.FieldType collectionElementType;
      @Nullable private Schema.FieldType mapKeyType;
      @Nullable private Schema.FieldType mapValueType;
      @Nullable private Schema rowSchema;
      @Nullable private byte[] metadata;

      Builder() {}

      private Builder(Schema.FieldType source) {
        this.typeName = source.getTypeName();
        this.collectionElementType = source.getCollectionElementType();
        this.mapKeyType = source.getMapKeyType();
        this.mapValueType = source.getMapValueType();
        this.rowSchema = source.getRowSchema();
        this.metadata = source.getMetadata();
      }

      Schema.FieldType.Builder setTypeName(Schema.TypeName typeName) {
        if (typeName == null) {
          throw new NullPointerException("Null typeName");
        }
        this.typeName = typeName;
        return this;
      }

      Schema.FieldType.Builder setCollectionElementType(
          @Nullable Schema.FieldType collectionElementType) {
        this.collectionElementType = collectionElementType;
        return this;
      }

      Schema.FieldType.Builder setMapKeyType(@Nullable Schema.FieldType mapKeyType) {
        this.mapKeyType = mapKeyType;
        return this;
      }

      Schema.FieldType.Builder setMapValueType(@Nullable Schema.FieldType mapValueType) {
        this.mapValueType = mapValueType;
        return this;
      }

      Schema.FieldType.Builder setRowSchema(@Nullable Schema rowSchema) {
        this.rowSchema = rowSchema;
        return this;
      }

      Schema.FieldType.Builder setMetadata(@Nullable byte[] metadata) {
        this.metadata = metadata;
        return this;
      }

      Schema.FieldType build() {
        String missing = "";
        if (this.typeName == null) {
          missing += " typeName";
        }
        if (!missing.isEmpty()) {
          throw new IllegalStateException("Missing required properties:" + missing);
        }
        return new FieldType(
            this.typeName,
            this.collectionElementType,
            this.mapKeyType,
            this.mapValueType,
            this.rowSchema,
            this.metadata);
      }
    }

    /** Create a {@link FieldType} for the given type. */
    public static FieldType of(TypeName typeName) {
      return forTypeName(typeName).build();
    }

    /** The type of string fields. */
    public static final FieldType STRING = FieldType.of(TypeName.STRING);

    /** The type of byte fields. */
    public static final FieldType BYTE = FieldType.of(TypeName.BYTE);

    /** The type of bytes fields. */
    public static final FieldType BYTES = FieldType.of(TypeName.BYTES);

    /** The type of int16 fields. */
    public static final FieldType INT16 = FieldType.of(TypeName.INT16);

    /** The type of int32 fields. */
    public static final FieldType INT32 = FieldType.of(TypeName.INT32);

    /** The type of int64 fields. */
    public static final FieldType INT64 = FieldType.of(TypeName.INT64);

    /** The type of float fields. */
    public static final FieldType FLOAT = FieldType.of(TypeName.FLOAT);

    /** The type of double fields. */
    public static final FieldType DOUBLE = FieldType.of(TypeName.DOUBLE);

    /** The type of decimal fields. */
    public static final FieldType DECIMAL = FieldType.of(TypeName.DECIMAL);

    /** The type of boolean fields. */
    public static final FieldType BOOLEAN = FieldType.of(TypeName.BOOLEAN);

    /** The type of datetime fields. */
    public static final FieldType DATETIME = FieldType.of(TypeName.DATETIME);

    /** Create an array type for the given field type. */
    public static FieldType array(FieldType elementType) {
      return FieldType.forTypeName(TypeName.ARRAY).setCollectionElementType(elementType).build();
    }

    /** Create a map type for the given key and value types. */
    public static FieldType map(FieldType keyType, FieldType valueType) {
      return FieldType.forTypeName(TypeName.MAP)
          .setMapKeyType(keyType)
          .setMapValueType(valueType)
          .build();
    }

    /** Create a map type for the given key and value types. */
    public static FieldType row(Schema schema) {
      return FieldType.forTypeName(TypeName.ROW).setRowSchema(schema).build();
    }

    /** Returns a copy of the descriptor with metadata set. */
    public FieldType withMetadata(@Nullable byte[] metadata) {
      return toBuilder().setMetadata(metadata).build();
    }

    /** Returns a copy of the descriptor with metadata set. */
    public FieldType withMetadata(String metadata) {
      return toBuilder().setMetadata(metadata.getBytes(StandardCharsets.UTF_8)).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof FieldType)) {
        return false;
      }
      FieldType other = (FieldType) o;
      return Objects.equals(getTypeName(), other.getTypeName())
          && Objects.equals(getCollectionElementType(), other.getCollectionElementType())
          && Objects.equals(getMapKeyType(), other.getMapKeyType())
          && Objects.equals(getMapValueType(), other.getMapValueType())
          && Objects.equals(getRowSchema(), other.getRowSchema())
          && Arrays.equals(getMetadata(), other.getMetadata());
    }

    private boolean equivalent(FieldType other) {
      if (!other.getTypeName().equals(getTypeName())) {
        return false;
      }
      switch (getTypeName()) {
        case ROW:
          if (!other.getRowSchema().equivalent(getRowSchema())) {
            return false;
          }
          break;
        case ARRAY:
          if (!other.getCollectionElementType().equivalent(getCollectionElementType())) {
            return false;
          }
          break;
        case MAP:
          if (!other.getMapKeyType().equivalent(getMapKeyType())
              || !other.getMapValueType().equivalent(getMapValueType())) {
            return false;
          }
          break;
        default:
          return other.equals(this);
      }
      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(
          new Object[] {
            getTypeName(),
            getCollectionElementType(),
            getMapKeyType(),
            getMapValueType(),
            getRowSchema(),
            getMetadata()
          });
    }
  }

  /** Field of a row. Contains the {@link FieldType} along with associated metadata. */
  @SuppressFBWarnings("NM_SAME_SIMPLE_NAME_AS_INTERFACE")
  public static final class Field
      implements Serializable, org.apache.beam.sdks.java.api.row.Schema.Field {
    private final String name;
    private final String description;
    private final Schema.FieldType type;
    private final Boolean nullable;

    private Field(String name, String description, Schema.FieldType type, Boolean nullable) {
      this.name = name;
      this.description = description;
      this.type = type;
      this.nullable = nullable;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public Schema.FieldType getType() {
      return type;
    }

    @Override
    public boolean getNullable() {
      return nullable == null || nullable;
    }

    @Override
    public String toString() {
      return "Field{"
          + "name="
          + name
          + ", "
          + "description="
          + description
          + ", "
          + "type="
          + type
          + ", "
          + "nullable="
          + nullable
          + "}";
    }

    /** @return a builder based on this instance state. */
    public Schema.Field.Builder toBuilder() {
      return new Builder(this);
    }

    /** {@link Schema.Field} builder. */
    public static final class Builder {
      @Nullable private String name;
      @Nullable private String description;
      @Nullable private Schema.FieldType type;
      @Nullable private Boolean nullable;

      Builder() {}

      private Builder(Schema.Field source) {
        this.name = source.getName();
        this.description = source.getDescription();
        this.type = source.getType();
        this.nullable = source.getNullable();
      }

      Schema.Field.Builder setName(String name) {
        if (name == null) {
          throw new NullPointerException("Null name");
        }
        this.name = name;
        return this;
      }

      Schema.Field.Builder setDescription(String description) {
        if (description == null) {
          throw new NullPointerException("Null description");
        }
        this.description = description;
        return this;
      }

      Schema.Field.Builder setType(Schema.FieldType type) {
        if (type == null) {
          throw new NullPointerException("Null type");
        }
        this.type = type;
        return this;
      }

      Schema.Field.Builder setNullable(Boolean nullable) {
        if (nullable == null) {
          throw new NullPointerException("Null nullable");
        }
        this.nullable = nullable;
        return this;
      }

      Schema.Field build() {
        String missing = "";
        if (this.name == null) {
          missing += " name";
        }
        if (this.description == null) {
          missing += " description";
        }
        if (this.type == null) {
          missing += " type";
        }
        if (this.nullable == null) {
          missing += " nullable";
        }
        if (!missing.isEmpty()) {
          throw new IllegalStateException("Missing required properties:" + missing);
        }
        return new Field(this.name, this.description, this.type, this.nullable);
      }
    }

    /** Return's a field with the give name and type. */
    public static Field of(String name, FieldType fieldType) {
      return new Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
          .setNullable(false) // By default fields are not nullable.
          .build();
    }

    /** Return's a nullable field with the give name and type. */
    public static Field nullable(String name, FieldType fieldType) {
      return new Field.Builder()
          .setName(name)
          .setDescription("")
          .setType(fieldType)
          .setNullable(true)
          .build();
    }

    /** Returns a copy of the Field with the name set. */
    public Field withName(String name) {
      return toBuilder().setName(name).build();
    }

    /** Returns a copy of the Field with the description set. */
    public Field withDescription(String description) {
      return toBuilder().setDescription(description).build();
    }

    /** Returns a copy of the Field with the {@link FieldType} set. */
    public Field withType(FieldType fieldType) {
      return toBuilder().setType(fieldType).build();
    }

    /** Returns a copy of the Field with isNullable set. */
    public Field withNullable(boolean isNullable) {
      return toBuilder().setNullable(isNullable).build();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Field)) {
        return false;
      }
      Field other = (Field) o;
      return Objects.equals(getName(), other.getName())
          && Objects.equals(getDescription(), other.getDescription())
          && Objects.equals(getType(), other.getType())
          && Objects.equals(getNullable(), other.getNullable());
    }

    private boolean equivalent(Field otherField, EquivalenceNullablePolicy nullablePolicy) {
      if (nullablePolicy == EquivalenceNullablePolicy.SAME
          && otherField.getNullable() == !getNullable()) {
        return false;
      } else if (nullablePolicy == EquivalenceNullablePolicy.WEAKEN) {
        if (getNullable() && !otherField.getNullable()) {
          return false;
        }
      }
      return otherField.getName().equals(getName()) && getType().equivalent(otherField.getType());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getDescription(), getType(), getNullable());
    }
  }

  /** Collects a stream of {@link Field}s into a {@link Schema}. */
  public static Collector<Field, List<Field>, Schema> toSchema() {
    return Collector.of(
        ArrayList::new,
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        Schema::fromFields);
  }

  private static Schema fromFields(List<Field> fields) {
    return new Schema(fields);
  }

  /** Return the list of all field names. */
  @Override
  public List<String> getFieldNames() {
    return getFields()
        .stream()
        .map(org.apache.beam.sdks.java.api.row.Schema.Field::getName)
        .collect(Collectors.toList());
  }

  /** Return a field by index. */
  @Override
  public Field getField(int index) {
    return Field.class.cast(getFields().get(index));
  }

  @Override
  public Field getField(String name) {
    return Field.class.cast(getFields().get(indexOf(name)));
  }

  /** Find the index of a given field. */
  @Override
  public int indexOf(String fieldName) {
    Integer index = fieldIndices.get(fieldName);
    if (index == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find field %s in schema %s", fieldName, this));
    }
    return index;
  }

  /** Returns true if {@code fieldName} exists in the schema, false otherwise. */
  public boolean hasField(String fieldName) {
    return fieldIndices.containsKey(fieldName);
  }

  /** Return the name of field by index. */
  @Override
  public String nameOf(int fieldIndex) {
    String name = fieldIndices.inverse().get(fieldIndex);
    if (name == null) {
      throw new IllegalArgumentException(String.format("Cannot find field %d", fieldIndex));
    }
    return name;
  }

  /** Return the count of fields. */
  public int getFieldCount() {
    return getFields().size();
  }
}
