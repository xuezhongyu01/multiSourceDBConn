package com.wayz.lbi.industry.datasource.db;

import java.util.Arrays;
import java.util.Optional;

public enum Type {
    MYSQL("mysql"),
    ES("es"),
    FAKE("fake"),
    ;

    private final String name;

    Type(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static Optional<Type> getByName(String name) {
        return Arrays.stream(Type.values()).filter(t -> t.name.equals(name)).findFirst();
    }
}
