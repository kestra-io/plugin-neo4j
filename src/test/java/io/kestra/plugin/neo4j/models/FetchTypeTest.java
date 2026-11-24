package io.kestra.plugin.neo4j.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FetchTypeTest {
    @Test
    void acceptsLegacyFetchOneValue() {
        assertEquals(FetchType.FETCH_ONE, FetchType.fromValue("FETCHONE"));
        assertEquals(FetchType.FETCH_ONE, FetchType.fromValue("fetchone"));
    }

    @Test
    void acceptsCurrentValues() {
        assertEquals(FetchType.FETCH, FetchType.fromValue("FETCH"));
        assertEquals(FetchType.FETCH_ONE, FetchType.fromValue("FETCH_ONE"));
    }

    @Test
    void deserializesLegacyAndCurrentValues() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        assertEquals(FetchType.FETCH_ONE, mapper.readValue("\"FETCHONE\"", FetchType.class));
        assertEquals(FetchType.FETCH_ONE, mapper.readValue("\"FETCH_ONE\"", FetchType.class));
    }
}
