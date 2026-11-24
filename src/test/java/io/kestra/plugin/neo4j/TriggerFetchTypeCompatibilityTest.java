package io.kestra.plugin.neo4j;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.neo4j.models.FetchType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TriggerFetchTypeCompatibilityTest {
    @SuppressWarnings("unchecked")
    private Property<FetchType> effectiveFetchType(Trigger trigger) throws Exception {
        Method method = Trigger.class.getDeclaredMethod("effectiveFetchType");
        method.setAccessible(true);
        return (Property<FetchType>) method.invoke(trigger);
    }

    @Test
    void usesLegacyStoreTypeWhenFetchTypeIsAbsent() throws Exception {
        Property<FetchType> legacy = Property.ofValue(FetchType.FETCH_ONE);
        Trigger trigger = Trigger.builder().storeType(legacy).build();

        assertSame(legacy, effectiveFetchType(trigger));
    }

    @Test
    void fetchTypeTakesPrecedenceOverStoreType() throws Exception {
        Property<FetchType> current = Property.ofValue(FetchType.FETCH);
        Property<FetchType> legacy = Property.ofValue(FetchType.STORE);
        Trigger trigger = Trigger.builder().fetchType(current).storeType(legacy).build();

        assertSame(current, effectiveFetchType(trigger));
    }

    @Test
    void defaultsToFetchWhenNeitherPropertyIsSet() throws Exception {
        Trigger trigger = Trigger.builder().build();

        assertEquals(Property.ofValue(FetchType.FETCH), effectiveFetchType(trigger));
    }
}
