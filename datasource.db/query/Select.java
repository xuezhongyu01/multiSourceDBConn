package com.wayz.lbi.industry.datasource.db.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Select {
    private String table;
    private List<Aggregation> aggregations;
    // and关系
    private List<Condition> conditions = Collections.emptyList();
    // or关系
    private List<Condition> orConditions = Collections.emptyList();
    private List<String> groups = Collections.emptyList();
    private List<Sort> sorts = Collections.emptyList();
    private long offset;
    private int limit;

    public Select addAggregation(Aggregation agg) {

        if (Objects.isNull(aggregations)) {
            aggregations = new ArrayList<>();
        }

        aggregations.add(agg);

        return this;
    }
}
