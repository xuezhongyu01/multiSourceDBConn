package com.wayz.lbi.industry.datasource.db;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Optional;

/**
 * field type of the datasource
 *
 * @createdAt 2019-11-17 09:52
 */
public enum FieldType {
    STRING("string", "s") {
        @Override
        public Object defaultValue() {
            return "";
        }
    },
    NUMBER("number", "n") {
        @Override
        public Object defaultValue() {
            return 0;
        }
    },
    DATETIME("datetime", "d") {
        @Override
        public Object defaultValue() {
            return LocalDateTime.of(0, 1, 1, 0, 0, 0);
        }
    },
    /**
     * this type is a placeholder, if the value is cannot recognized
     */
    INVALID("invalid", "") {
        @Override
        public Object defaultValue() { throw new UnsupportedOperationException(); }

        @Override
        public String getName() { throw new UnsupportedOperationException(); }

        @Override
        public String getUniqueSimpleId() { throw new UnsupportedOperationException(); }

        @Override
        public String fieldId(int suffix) { throw new UnsupportedOperationException(); }
    },
    ;
    private final String name;
    private final String uniqueSimpleId;

    public abstract Object defaultValue();

    FieldType(String name, String uniqueSimpleId) {
        this.name = name;
        this.uniqueSimpleId = uniqueSimpleId;
    }

    public String getName() {
        return name;
    }

    public String getUniqueSimpleId() {
        return uniqueSimpleId;
    }

    public String fieldId(int suffix) {
        return String.format("%s%d", getUniqueSimpleId(), suffix);
    }

    public static Optional<FieldType> getByName(String name) {
        return Arrays.stream(FieldType.values()).filter(t -> t.getName().equals(name)).findFirst();
    }
}
