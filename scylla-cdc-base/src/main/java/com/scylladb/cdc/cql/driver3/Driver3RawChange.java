package com.scylladb.cdc.cql.driver3;

import static com.datastax.driver.core.Metadata.quoteIfNecessary;

import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.base.Preconditions;
import com.scylladb.cdc.model.StreamId;
import com.scylladb.cdc.model.cql.Field;
import com.scylladb.cdc.model.worker.RawChange;
import com.scylladb.cdc.model.worker.ChangeId;
import com.scylladb.cdc.model.worker.ChangeSchema;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class Driver3RawChange implements RawChange {
    private final Row row;
    private final ChangeSchema schema;

    public Driver3RawChange(Row row, ChangeSchema schema) {
        this.row = Preconditions.checkNotNull(row);
        this.schema = Preconditions.checkNotNull(schema);
    }

    @Override
    public ChangeId getId() {
        return new ChangeId(new StreamId(row.getBytes(quoteIfNecessary("cdc$stream_id"))),
                row.getUUID(quoteIfNecessary("cdc$time")));
    }

    @Override
    public ChangeSchema getSchema() {
        return schema;
    }

    @Override
    public Object getAsObject(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            // TODO - check if quoteIfNecessary is needed here in getObject()
            ChangeSchema.ColumnDefinition columnDefinition = schema.getColumnDefinition(columnName);
            return translateDriverObject(row.getObject(columnName), columnDefinition.getCdcLogDataType());
        }
    }

    private Object translateDriverObject(Object driverObject, ChangeSchema.DataType dataType) {
        // Some types returned by getObject() are
        // some classes of Java Driver. We should
        // translate them into a non-Java-Driver-specific
        // types. (for example UDTValue)

        if (driverObject == null) {
            return null;
        }

        // Even though List, Map, Set are
        // a standard Java library types,
        // they can contain values that
        // are Java-Driver-specific.
        //
        // For example: map<int, some_udt>
        //
        // Moreover, we will box them into Field.
        // Recursively call translateObject on their elements.

        // TODO - use dataType instead of instanceof
        if (driverObject instanceof List) {
            List<Object> driverList = (List<Object>) driverObject;
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);

            return driverList.stream().map(o -> translateDriverObject(o, innerType))
                    .map(o -> new Field(innerType, o)).collect(Collectors.toList());
        } else if (driverObject instanceof Set) {
            Set<Object> driverSet = (Set<Object>) driverObject;
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);

            // Deliberately using LinkedHashSet to preserve the same order as driverObject.
            return driverSet.stream().map(o -> translateDriverObject(o, innerType))
                    .map(o -> new Field(innerType, o)).collect(Collectors.toCollection(LinkedHashSet::new));
        } else if (driverObject instanceof Map) {
            Map<Object, Object> driverMap = (Map<Object, Object>) driverObject;
            ChangeSchema.DataType keyType = dataType.getTypeArguments().get(0);
            ChangeSchema.DataType valueType = dataType.getTypeArguments().get(1);

            // Deliberately using LinkedHashMap to preserve the same order as driverObject.
            Map<Field, Field> translatedMap = new LinkedHashMap<>();
            for (Map.Entry<Object, Object> entry : driverMap.entrySet()) {
                Field key = new Field(keyType, translateDriverObject(entry.getKey(), keyType));
                Field value = new Field(valueType, translateDriverObject(entry.getValue(), valueType));
                translatedMap.put(key, value);
            }
            return translatedMap;
        }

        // Java-Driver-specific types
        if (driverObject instanceof UDTValue) {
            UDTValue driverUDT = (UDTValue) driverObject;

            Map<String, Field> translatedMap = new LinkedHashMap<>();
            for (Map.Entry<String, ChangeSchema.DataType> entry : dataType.getFields().entrySet()) {
                Object translatedObject = translateDriverObject(driverUDT.getObject(entry.getKey()), entry.getValue());
                Field field = new Field(entry.getValue(), translatedObject);
                translatedMap.put(entry.getKey(), field);
            }
            return translatedMap;
        } else if (driverObject instanceof com.datastax.driver.core.Duration) {
            com.datastax.driver.core.Duration driverDuration = (com.datastax.driver.core.Duration) driverObject;
            return new Duration(driverDuration.getMonths(), driverDuration.getDays(), driverDuration.getNanoseconds());
        } else if (driverObject instanceof LocalDate) {
            LocalDate driverDate = (LocalDate) driverObject;
            return new CqlDate(driverDate.getYear(), driverDate.getMonth(), driverDate.getMonth());
        }

        // No translation needed, the object is
        // already of a good type.
        return driverObject;
    }

    @Override
    public ByteBuffer getAsBytes(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getBytesUnsafe(columnName);
        }
    }

    /*
     * What follows are temporary methods
     * used for porting the replicator
     * from old library to new library.
     *
     * Those methods should be removed
     * after the porting process is done.
     */

    @Deprecated
    public Object TEMPORARY_PORTING_getAsDriverObject(String columnName) {
        if (row.isNull(columnName)) {
            return null;
        } else {
            return row.getObject(columnName);
        }
    }

    @Override
    public boolean TEMPORARY_PORTING_isDeleted(String name) {
        String deletionColumnName = "cdc$deleted_" + name;
        return !row.isNull(deletionColumnName) && row.getBool(deletionColumnName);
    }
}
