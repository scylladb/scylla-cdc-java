package com.scylladb.cdc.cql;

import com.scylladb.cdc.cql.CQLConfiguration.AddressTranslatorType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CQLConfigurationTest {
    private static CQLConfiguration.Builder builderWithContactPoint() {
        return CQLConfiguration.builder().addContactPoint("127.0.0.1");
    }

    @Test
    public void testAddressTranslatorDefaultsToNone() {
        CQLConfiguration configuration = builderWithContactPoint().build();
        assertEquals(AddressTranslatorType.NONE, configuration.getAddressTranslator());
    }

    @Test
    public void testAddressTranslatorEc2MultiRegion() {
        CQLConfiguration configuration = builderWithContactPoint()
                .withAddressTranslator(AddressTranslatorType.EC2_MULTI_REGION)
                .build();
        assertEquals(AddressTranslatorType.EC2_MULTI_REGION, configuration.getAddressTranslator());
    }

    @Test
    public void testAddressTranslatorExplicitNone() {
        CQLConfiguration configuration = builderWithContactPoint()
                .withAddressTranslator(AddressTranslatorType.EC2_MULTI_REGION)
                .withAddressTranslator(AddressTranslatorType.NONE)
                .build();
        assertEquals(AddressTranslatorType.NONE, configuration.getAddressTranslator());
    }

    @Test
    public void testAddressTranslatorRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> builderWithContactPoint().withAddressTranslator(null));
    }
}
