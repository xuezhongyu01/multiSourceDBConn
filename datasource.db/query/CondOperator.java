package com.wayz.lbi.industry.datasource.db.query;

import java.util.Arrays;
import java.util.Optional;

public enum CondOperator {

    /**
     * equal condition operator
     */
    EQ("="),
    /**
     * in condition operator
     */
    IN("in"),
    /**
     * like condition operator
     */
    LIKE("like"),
    /**
     * geo distance in condition operator ,meter as unit
     */
    GEO_IN("geo_in"),
    /**
     * geo within condition operator
     */
    GEO_WITHIN("geo_within"),
    ;

    private final String name;

    CondOperator(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Optional<CondOperator> getByName(String name) {

        return Arrays.stream(CondOperator.values()).filter(o -> o.getName().equals(name)).findFirst();
    }
}
