package com.wayz.lbi.industry.datasource.db.query;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public final class Aggregation {
    private Aggregator aggregator;
    private String fieldId;

    public static Aggregation id(String fieldId) {
        return new Aggregation().setAggregator(Aggregator.ID).setFieldId(fieldId);
    }
}
