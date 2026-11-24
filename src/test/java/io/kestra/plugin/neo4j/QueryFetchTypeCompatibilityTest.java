package io.kestra.plugin.neo4j;

import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.neo4j.models.FetchType;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class QueryFetchTypeCompatibilityTest {
    @SuppressWarnings("unchecked")
    private Property<FetchType> effectiveFetchType(Query query) throws Exception {
        Method method = Query.class.getDeclaredMethod("effectiveFetchType");
        method.setAccessible(true);
        return (Property<FetchType>) method.invoke(query);
    }

    @Test
    void usesLegacyStoreTypeWhenFetchTypeIsAbsent() throws Exception {
        Property<FetchType> legacy = Property.ofValue(FetchType.FETCH_ONE);
        Query query = Query.builder().storeType(legacy).build();

        assertSame(legacy, effectiveFetchType(query));
    }

    @Test
    void fetchTypeTakesPrecedenceOverStoreType() throws Exception {
        Property<FetchType> current = Property.ofValue(FetchType.FETCH);
        Property<FetchType> legacy = Property.ofValue(FetchType.STORE);
        Query query = Query.builder().fetchType(current).storeType(legacy).build();

        assertSame(current, effectiveFetchType(query));
    }

    @Test
    void defaultsToNoneWhenNeitherPropertyIsSet() throws Exception {
        Query query = Query.builder().build();

        assertEquals(Property.ofValue(FetchType.NONE), effectiveFetchType(query));
    }
}
