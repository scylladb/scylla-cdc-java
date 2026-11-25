package com.scylladb.cdc.cql.driver4;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.scylladb.cdc.model.worker.ChangeSchema;
import com.scylladb.cdc.model.worker.cql.CqlDate;
import com.scylladb.cdc.model.worker.cql.CqlDuration;
import com.scylladb.cdc.model.worker.cql.Field;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Turns a value read with Java driver 4.x into the type scylla-cdc-driver3 returns for it, which
 * is the type the {@link Field} getters cast to.
 */
final class Driver4ToLibraryTranslator {
    private Driver4ToLibraryTranslator() {
    }

    static Object translate(Object driverObject, ChangeSchema.DataType dataType) {
        if (driverObject == null) {
            return null;
        }
        // Collection elements are translated and wrapped in a Field, keeping the driver's order.
        if (driverObject instanceof List) {
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);
            return ((List<?>) driverObject).stream()
                    .map(o -> new Driver4Field(innerType, translate(o, innerType)))
                    .collect(Collectors.toList());
        } else if (driverObject instanceof Set) {
            ChangeSchema.DataType innerType = dataType.getTypeArguments().get(0);
            return ((Set<?>) driverObject).stream()
                    .map(o -> new Driver4Field(innerType, translate(o, innerType)))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } else if (driverObject instanceof Map) {
            ChangeSchema.DataType keyType = dataType.getTypeArguments().get(0);
            ChangeSchema.DataType valueType = dataType.getTypeArguments().get(1);
            Map<Field, Field> translated = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) driverObject).entrySet()) {
                translated.put(new Driver4Field(keyType, translate(entry.getKey(), keyType)),
                        new Driver4Field(valueType, translate(entry.getValue(), valueType)));
            }
            return translated;
        } else if (driverObject instanceof UdtValue) {
            UdtValue udt = (UdtValue) driverObject;
            Map<String, Field> translated = new LinkedHashMap<>();
            // By position: the schema lists the fields in the type's order.
            int index = 0;
            for (Map.Entry<String, ChangeSchema.DataType> field : dataType.getUdtType().getFields().entrySet()) {
                translated.put(field.getKey(),
                        new Driver4Field(field.getValue(), translate(udt.getObject(index++), field.getValue())));
            }
            return translated;
        } else if (driverObject instanceof TupleValue) {
            TupleValue tuple = (TupleValue) driverObject;
            List<Field> translated = new ArrayList<>();
            int index = 0;
            for (ChangeSchema.DataType fieldType : dataType.getTypeArguments()) {
                translated.add(new Driver4Field(fieldType, translate(tuple.getObject(index++), fieldType)));
            }
            return translated;
        } else if (driverObject instanceof com.datastax.oss.driver.api.core.data.CqlDuration) {
            com.datastax.oss.driver.api.core.data.CqlDuration duration =
                    (com.datastax.oss.driver.api.core.data.CqlDuration) driverObject;
            return new CqlDuration(duration.getMonths(), duration.getDays(), duration.getNanoseconds());
        } else if (driverObject instanceof LocalDate) {
            LocalDate date = (LocalDate) driverObject;
            return new CqlDate(date.getYear(), date.getMonthValue(), date.getDayOfMonth());
        } else if (driverObject instanceof Instant) {
            return Date.from((Instant) driverObject);
        } else if (driverObject instanceof LocalTime) {
            return ((LocalTime) driverObject).toNanoOfDay();
        }
        // Every other type is already the one 3.x returns.
        return driverObject;
    }
}
