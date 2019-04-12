/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.apache.pulsar.client.avro.generated;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class NasaMission extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 855396714074990031L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NasaMission\",\"namespace\":\"org.apache.pulsar.client.avro.generated\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"create_year\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"create_time\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},{\"name\":\"create_timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<NasaMission> ENCODER =
      new BinaryMessageEncoder<NasaMission>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<NasaMission> DECODER =
      new BinaryMessageDecoder<NasaMission>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   */
  public static BinaryMessageDecoder<NasaMission> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   */
  public static BinaryMessageDecoder<NasaMission> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<NasaMission>(MODEL$, SCHEMA$, resolver);
  }

  /** Serializes this NasaMission to a ByteBuffer. */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /** Deserializes a NasaMission from a ByteBuffer. */
  public static NasaMission fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  @Deprecated public int id;
  @Deprecated public java.lang.CharSequence name;
  @Deprecated public org.joda.time.LocalDate create_year;
  @Deprecated public org.joda.time.LocalTime create_time;
  @Deprecated public org.joda.time.DateTime create_timestamp;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public NasaMission() {}

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param name The new value for name
   * @param create_year The new value for create_year
   * @param create_time The new value for create_time
   * @param create_timestamp The new value for create_timestamp
   */
  public NasaMission(java.lang.Integer id, java.lang.CharSequence name, org.joda.time.LocalDate create_year, org.joda.time.LocalTime create_time, org.joda.time.DateTime create_timestamp) {
    this.id = id;
    this.name = name;
    this.create_year = create_year;
    this.create_time = create_time;
    this.create_timestamp = create_timestamp;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return name;
    case 2: return create_year;
    case 3: return create_time;
    case 4: return create_timestamp;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  protected static final org.apache.avro.data.TimeConversions.DateConversion DATE_CONVERSION = new org.apache.avro.data.TimeConversions.DateConversion();
  protected static final org.apache.avro.data.TimeConversions.TimeConversion TIME_CONVERSION = new org.apache.avro.data.TimeConversions.TimeConversion();
  protected static final org.apache.avro.data.TimeConversions.TimestampConversion TIMESTAMP_CONVERSION = new org.apache.avro.data.TimeConversions.TimestampConversion();
  protected static final org.apache.avro.Conversions.DecimalConversion DECIMAL_CONVERSION = new org.apache.avro.Conversions.DecimalConversion();

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      null,
      null,
      DATE_CONVERSION,
      TIME_CONVERSION,
      TIMESTAMP_CONVERSION,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    case 1: name = (java.lang.CharSequence)value$; break;
    case 2: create_year = (org.joda.time.LocalDate)value$; break;
    case 3: create_time = (org.joda.time.LocalTime)value$; break;
    case 4: create_timestamp = (org.joda.time.DateTime)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public java.lang.Integer getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(java.lang.Integer value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public java.lang.CharSequence getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(java.lang.CharSequence value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'create_year' field.
   * @return The value of the 'create_year' field.
   */
  public org.joda.time.LocalDate getCreateYear() {
    return create_year;
  }

  /**
   * Sets the value of the 'create_year' field.
   * @param value the value to set.
   */
  public void setCreateYear(org.joda.time.LocalDate value) {
    this.create_year = value;
  }

  /**
   * Gets the value of the 'create_time' field.
   * @return The value of the 'create_time' field.
   */
  public org.joda.time.LocalTime getCreateTime() {
    return create_time;
  }

  /**
   * Sets the value of the 'create_time' field.
   * @param value the value to set.
   */
  public void setCreateTime(org.joda.time.LocalTime value) {
    this.create_time = value;
  }

  /**
   * Gets the value of the 'create_timestamp' field.
   * @return The value of the 'create_timestamp' field.
   */
  public org.joda.time.DateTime getCreateTimestamp() {
    return create_timestamp;
  }

  /**
   * Sets the value of the 'create_timestamp' field.
   * @param value the value to set.
   */
  public void setCreateTimestamp(org.joda.time.DateTime value) {
    this.create_timestamp = value;
  }

  /**
   * Creates a new NasaMission RecordBuilder.
   * @return A new NasaMission RecordBuilder
   */
  public static org.apache.pulsar.client.avro.generated.NasaMission.Builder newBuilder() {
    return new org.apache.pulsar.client.avro.generated.NasaMission.Builder();
  }

  /**
   * Creates a new NasaMission RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new NasaMission RecordBuilder
   */
  public static org.apache.pulsar.client.avro.generated.NasaMission.Builder newBuilder(org.apache.pulsar.client.avro.generated.NasaMission.Builder other) {
    return new org.apache.pulsar.client.avro.generated.NasaMission.Builder(other);
  }

  /**
   * Creates a new NasaMission RecordBuilder by copying an existing NasaMission instance.
   * @param other The existing instance to copy.
   * @return A new NasaMission RecordBuilder
   */
  public static org.apache.pulsar.client.avro.generated.NasaMission.Builder newBuilder(org.apache.pulsar.client.avro.generated.NasaMission other) {
    return new org.apache.pulsar.client.avro.generated.NasaMission.Builder(other);
  }

  /**
   * RecordBuilder for NasaMission instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<NasaMission>
    implements org.apache.avro.data.RecordBuilder<NasaMission> {

    private int id;
    private java.lang.CharSequence name;
    private org.joda.time.LocalDate create_year;
    private org.joda.time.LocalTime create_time;
    private org.joda.time.DateTime create_timestamp;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.apache.pulsar.client.avro.generated.NasaMission.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.create_year)) {
        this.create_year = data().deepCopy(fields()[2].schema(), other.create_year);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.create_time)) {
        this.create_time = data().deepCopy(fields()[3].schema(), other.create_time);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.create_timestamp)) {
        this.create_timestamp = data().deepCopy(fields()[4].schema(), other.create_timestamp);
        fieldSetFlags()[4] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing NasaMission instance
     * @param other The existing instance to copy.
     */
    private Builder(org.apache.pulsar.client.avro.generated.NasaMission other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.name)) {
        this.name = data().deepCopy(fields()[1].schema(), other.name);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.create_year)) {
        this.create_year = data().deepCopy(fields()[2].schema(), other.create_year);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.create_time)) {
        this.create_time = data().deepCopy(fields()[3].schema(), other.create_time);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.create_timestamp)) {
        this.create_timestamp = data().deepCopy(fields()[4].schema(), other.create_timestamp);
        fieldSetFlags()[4] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public java.lang.Integer getId() {
      return id;
    }

    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder setId(int value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder clearId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'name' field.
      * @return The value.
      */
    public java.lang.CharSequence getName() {
      return name;
    }

    /**
      * Sets the value of the 'name' field.
      * @param value The value of 'name'.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder setName(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.name = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'name' field has been set.
      * @return True if the 'name' field has been set, false otherwise.
      */
    public boolean hasName() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'name' field.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder clearName() {
      name = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'create_year' field.
      * @return The value.
      */
    public org.joda.time.LocalDate getCreateYear() {
      return create_year;
    }

    /**
      * Sets the value of the 'create_year' field.
      * @param value The value of 'create_year'.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder setCreateYear(org.joda.time.LocalDate value) {
      validate(fields()[2], value);
      this.create_year = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'create_year' field has been set.
      * @return True if the 'create_year' field has been set, false otherwise.
      */
    public boolean hasCreateYear() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'create_year' field.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder clearCreateYear() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'create_time' field.
      * @return The value.
      */
    public org.joda.time.LocalTime getCreateTime() {
      return create_time;
    }

    /**
      * Sets the value of the 'create_time' field.
      * @param value The value of 'create_time'.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder setCreateTime(org.joda.time.LocalTime value) {
      validate(fields()[3], value);
      this.create_time = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'create_time' field has been set.
      * @return True if the 'create_time' field has been set, false otherwise.
      */
    public boolean hasCreateTime() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'create_time' field.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder clearCreateTime() {
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'create_timestamp' field.
      * @return The value.
      */
    public org.joda.time.DateTime getCreateTimestamp() {
      return create_timestamp;
    }

    /**
      * Sets the value of the 'create_timestamp' field.
      * @param value The value of 'create_timestamp'.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder setCreateTimestamp(org.joda.time.DateTime value) {
      validate(fields()[4], value);
      this.create_timestamp = value;
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'create_timestamp' field has been set.
      * @return True if the 'create_timestamp' field has been set, false otherwise.
      */
    public boolean hasCreateTimestamp() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'create_timestamp' field.
      * @return This builder.
      */
    public org.apache.pulsar.client.avro.generated.NasaMission.Builder clearCreateTimestamp() {
      fieldSetFlags()[4] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public NasaMission build() {
      try {
        NasaMission record = new NasaMission();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Integer) defaultValue(fields()[0], record.getConversion(0));
        record.name = fieldSetFlags()[1] ? this.name : (java.lang.CharSequence) defaultValue(fields()[1], record.getConversion(1));
        record.create_year = fieldSetFlags()[2] ? this.create_year : (org.joda.time.LocalDate) defaultValue(fields()[2], record.getConversion(2));
        record.create_time = fieldSetFlags()[3] ? this.create_time : (org.joda.time.LocalTime) defaultValue(fields()[3], record.getConversion(3));
        record.create_timestamp = fieldSetFlags()[4] ? this.create_timestamp : (org.joda.time.DateTime) defaultValue(fields()[4], record.getConversion(4));
        return record;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<NasaMission>
    WRITER$ = (org.apache.avro.io.DatumWriter<NasaMission>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<NasaMission>
    READER$ = (org.apache.avro.io.DatumReader<NasaMission>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
