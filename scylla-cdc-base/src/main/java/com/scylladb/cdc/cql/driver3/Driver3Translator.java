package com.scylladb.cdc.cql.driver3;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.cql.Cell;
import com.scylladb.cdc.model.cql.Field;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.RawChange;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Translates library objects (for example those
 * returned by RawChange::getAsObject) into object
 * types returned by Java Driver (version 3).
 */
public class Driver3Translator {
    private final Metadata clusterMetadata;

    public Driver3Translator(Metadata clusterMetadata) {
        this.clusterMetadata = Preconditions.checkNotNull(clusterMetadata);
    }

    public Object translate(Cell cell) {
        return translate(cell.getAsObject(), cell.getColumnDefinition().getCdcLogDataType());
    }

    public Object translate(Object libraryObject, ChangeSchema.DataType libraryDataType) {
        if (libraryObject == null) {
            return null;
        }

        switch (libraryDataType.getCqlType()) {
            // Unboxing from Field: LIST, SET, MAP
            case LIST: {
                List<Field> libraryList = (List<Field>) libraryObject;
                ChangeSchema.DataType innerType = libraryDataType.getTypeArguments().get(0);
                return libraryList.stream().map(f -> translate(f.getAsObject(), innerType)).collect(Collectors.toList());
            }
            case SET: {
                Set<Field> librarySet = (Set<Field>) libraryObject;
                ChangeSchema.DataType innerType = libraryDataType.getTypeArguments().get(0);
                return librarySet.stream().map(f -> translate(f.getAsObject(), innerType)).collect(Collectors.toCollection(LinkedHashSet::new));
            }
            case MAP: {
                Map<Field, Field> libraryMap = (Map<Field, Field>) libraryObject;
                ChangeSchema.DataType keyType = libraryDataType.getTypeArguments().get(0);
                ChangeSchema.DataType valueType = libraryDataType.getTypeArguments().get(1);

                Map<Object, Object> translatedMap = new LinkedHashMap<>();
                for (Map.Entry<Field, Field> entry : libraryMap.entrySet()) {
                    Object translatedKey = translate(entry.getKey().getAsObject(), keyType);
                    Object translatedValue = translate(entry.getValue().getAsObject(), valueType);
                    translatedMap.put(translatedKey, translatedValue);
                }
                return translatedMap;
            }
            // Java-Driver-specific types
            case UDT: {
                Map<String, Field> libraryUDT = (Map<String, Field>) libraryObject;
                UserType driverUDTType = clusterMetadata.getKeyspace(libraryDataType.getUdtType().getKeyspace()).getUserType(libraryDataType.getUdtType().getName());
                UDTValue driverUDTValue = driverUDTType.newValue();
                for (Map.Entry<String, ChangeSchema.DataType> entry : libraryDataType.getUdtType().getFields().entrySet()) {
                    Object fieldObject = translate(libraryUDT.get(entry.getKey()).getAsObject(), entry.getValue());
                    driverUDTValue.set(entry.getKey(), fieldObject, getTypeCodec(driverUDTType.getFieldType(entry.getKey())));
                }
                return driverUDTValue;
            }
            case TUPLE: {
                List<Field> libraryTuple = (List<Field>) libraryObject;
                TupleType driverTupleType = (TupleType) getDriverDataType(libraryDataType);
                TupleValue driverTupleValue = driverTupleType.newValue();
                for (int i = 0; i < libraryTuple.size(); i++) {
                    Field field = libraryTuple.get(i);
                    ChangeSchema.DataType fieldLibraryType = field.getDataType();
                    DataType fieldDriverType = getDriverDataType(fieldLibraryType);
                    Object driverObject = translate(field.getAsObject(), fieldLibraryType);
                    driverTupleValue.set(i, driverObject, getTypeCodec(fieldDriverType));
                }
                return driverTupleValue;
            }
            case DURATION: {
                RawChange.Duration libraryDuration = (RawChange.Duration) libraryObject;
                return Duration.newInstance(libraryDuration.getMonths(), libraryDuration.getDays(), libraryDuration.getNanoseconds());
            }
            case DATE: {
                RawChange.CqlDate libraryDate = (RawChange.CqlDate) libraryObject;
                return LocalDate.fromYearMonthDay(libraryDate.getYear(), libraryDate.getMonth(), libraryDate.getDay());
            }
            default:
                // No translation needed, the object is
                // already of a good type.
                return libraryObject;
        }
    }

    private TypeCodec<Object> getTypeCodec(DataType driverDataType) {
        return CodecRegistry.DEFAULT_INSTANCE.codecFor(driverDataType);
    }

    private DataType getDriverDataType(ChangeSchema.DataType libraryDataType) {
        switch (libraryDataType.getCqlType()) {
            case ASCII:
                return DataType.ascii();
            case BIGINT:
                return DataType.bigint();
            case BLOB:
                return DataType.blob();
            case BOOLEAN:
                return DataType.cboolean();
            case COUNTER:
                return DataType.counter();
            case DECIMAL:
                return DataType.decimal();
            case DOUBLE:
                return DataType.cdouble();
            case FLOAT:
                return DataType.cfloat();
            case INT:
                return DataType.cint();
            case TEXT:
                return DataType.text();
            case TIMESTAMP:
                return DataType.timestamp();
            case UUID:
                return DataType.uuid();
            case VARCHAR:
                return DataType.varchar();
            case VARINT:
                return DataType.varint();
            case TIMEUUID:
                return DataType.timeuuid();
            case INET:
                return DataType.inet();
            case DATE:
                return DataType.date();
            case TIME:
                return DataType.time();
            case SMALLINT:
                return DataType.smallint();
            case TINYINT:
                return DataType.tinyint();
            case DURATION:
                return DataType.duration();
            case LIST: {
                DataType innerType = getDriverDataType(libraryDataType.getTypeArguments().get(0));
                return DataType.list(innerType);
            }
            case MAP: {
                DataType keyType = getDriverDataType(libraryDataType.getTypeArguments().get(0));
                DataType valueType = getDriverDataType(libraryDataType.getTypeArguments().get(1));
                return DataType.map(keyType, valueType);
            }
            case SET: {
                DataType innerType = getDriverDataType(libraryDataType.getTypeArguments().get(0));
                return DataType.set(innerType);
            }
            case UDT: {
                return clusterMetadata.getKeyspace(libraryDataType.getUdtType().getKeyspace())
                        .getUserType(libraryDataType.getUdtType().getName());
            }
            case TUPLE: {
                List<DataType> innerTypes = libraryDataType.getTypeArguments().stream()
                        .map(this::getDriverDataType).collect(Collectors.toList());
                return clusterMetadata.newTupleType(innerTypes);
            }
            default:
                throw new IllegalArgumentException("Library type " + libraryDataType + " is not supported.");
        }
    }
}
