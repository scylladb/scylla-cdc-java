package com.scylladb.cdc.replicator.driver3;

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
import com.scylladb.cdc.model.worker.cql.Cell;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.CqlDuration;
import com.scylladb.cdc.model.worker.cql.Field;
import com.scylladb.cdc.model.worker.ChangeSchema;

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
public class Driver3FromLibraryTranslator {
    private final Metadata clusterMetadata;

    public Driver3FromLibraryTranslator(Metadata clusterMetadata) {
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
                List<?> libraryList = (List<?>) libraryObject;
                ChangeSchema.DataType innerType = libraryDataType.getTypeArguments().get(0);
                return libraryList.stream().map(f -> translate(((Field)f).getAsObject(), innerType)).collect(Collectors.toList());
            }
            case SET: {
                Set<?> librarySet = (Set<?>) libraryObject;
                ChangeSchema.DataType innerType = libraryDataType.getTypeArguments().get(0);
                return librarySet.stream().map(f -> translate(((Field)f).getAsObject(), innerType)).collect(Collectors.toCollection(LinkedHashSet::new));
            }
            case MAP: {
                Map<?, ?> libraryMap = (Map<?, ?>) libraryObject;
                ChangeSchema.DataType keyType = libraryDataType.getTypeArguments().get(0);
                ChangeSchema.DataType valueType = libraryDataType.getTypeArguments().get(1);

                Map<Object, Object> translatedMap = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : libraryMap.entrySet()) {
                    Object translatedKey = translate(((Field)entry.getKey()).getAsObject(), keyType);
                    Object translatedValue = translate(((Field)entry.getValue()).getAsObject(), valueType);
                    translatedMap.put(translatedKey, translatedValue);
                }
                return translatedMap;
            }
            // Java-Driver-specific types
            case UDT: {
                Map<?, ?> libraryUDT = (Map<?, ?>) libraryObject;
                UserType driverUDTType = clusterMetadata.getKeyspace(libraryDataType.getUdtType().getKeyspace()).getUserType(libraryDataType.getUdtType().getName());
                UDTValue driverUDTValue = driverUDTType.newValue();
                for (Map.Entry<String, ChangeSchema.DataType> entry : libraryDataType.getUdtType().getFields().entrySet()) {
                    Object fieldObject = translate(((Field)libraryUDT.get(entry.getKey())).getAsObject(), entry.getValue());
                    driverUDTValue.set(entry.getKey(), fieldObject, getTypeCodec(driverUDTType.getFieldType(entry.getKey())));
                }
                return driverUDTValue;
            }
            case TUPLE: {
                List<?> libraryTuple = (List<?>) libraryObject;
                TupleType driverTupleType = (TupleType) getDriverDataType(libraryDataType);
                TupleValue driverTupleValue = driverTupleType.newValue();
                for (int i = 0; i < libraryTuple.size(); i++) {
                    Field field = (Field)libraryTuple.get(i);
                    ChangeSchema.DataType fieldLibraryType = field.getDataType();
                    DataType fieldDriverType = getDriverDataType(fieldLibraryType);
                    Object driverObject = translate(field.getAsObject(), fieldLibraryType);
                    driverTupleValue.set(i, driverObject, getTypeCodec(fieldDriverType));
                }
                return driverTupleValue;
            }
            case DURATION: {
                CqlDuration libraryDuration = (CqlDuration) libraryObject;
                return Duration.newInstance((int) libraryDuration.getMonths(), (int) libraryDuration.getDays(), libraryDuration.getNanoseconds());
            }
            case DATE: {
                CqlDate libraryDate = (CqlDate) libraryObject;
                return LocalDate.fromYearMonthDay(libraryDate.getYear(), libraryDate.getMonth(), libraryDate.getDay());
            }
            default:
                // No translation needed, the object is
                // already of a good type.
                return libraryObject;
        }
    }

    public TypeCodec<Object> getTypeCodec(ChangeSchema.DataType libraryDataType) {
        DataType driverDataType = getDriverDataType(libraryDataType);
        return getTypeCodec(driverDataType);
    }

    private TypeCodec<Object> getTypeCodec(DataType driverDataType) {
        return CodecRegistry.DEFAULT_INSTANCE.codecFor(driverDataType);
    }

    public DataType getDriverDataType(ChangeSchema.DataType libraryDataType) {
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
