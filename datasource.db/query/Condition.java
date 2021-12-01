package com.wayz.lbi.industry.datasource.db.query;

import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@AllArgsConstructor
@Accessors(chain = true)
public class Condition {
    private CondOperator op;
    private String fieldId;
    private Object value;

    public static Condition eq(String fieldId, Object value) {
        return new Condition(CondOperator.EQ, fieldId, value);
    }

    public static Condition in(String fieldId, Collection<?> value) {
        return new Condition(CondOperator.IN, fieldId, value);
    }

    public static Condition like(String fieldId, String value) {
        return new Condition(CondOperator.LIKE, fieldId, value);
    }

}
