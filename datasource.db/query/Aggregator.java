package com.wayz.lbi.industry.datasource.db.query;

import java.util.Arrays;
import java.util.Optional;

/**
 * @createdAt 2019-11-19 17:39:29
 */
public enum Aggregator {
    SUM("sum", true),
    COUNT("count", true),
    AVG("avg", true),
    MAX("max", true),
    MIN("min", true),
    LIST("list", false),
    DISTINCT("distinct", false),
    /**
     * do nothing, returns the field
     */
    ID("id", false),
    ;
    private final String name;
    /**
     * the auxiable aggregator, cannot see by the user, cannot be used as updating value
     */
    private final boolean editable;

    Aggregator(String name, boolean editable) {
        this.name = name;
        this.editable = editable;
    }

    public String getName() {
        return name;
    }

    public boolean isEditable() {
        return editable;
    }

    public static Optional<Aggregator> getByName(String name) {
        return Arrays.stream(Aggregator.values()).filter(a -> a.getName().equals(name)).findFirst();
    }
}
