package io.kestra.plugin.neo4j.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.util.Locale;

public enum FetchType {
    STORE,
    FETCH,
    FETCH_ONE,
    NONE;

    @JsonCreator
    public static FetchType fromValue(String value) {
        if (value == null) {
            return null;
        }
        if ("FETCHONE".equalsIgnoreCase(value)) {
            return FETCH_ONE;
        }
        return FetchType.valueOf(value.toUpperCase(Locale.ROOT));
    }
}
