package com.wayz.lbi.industry.datasource.db.query;

import java.util.Map;

import lombok.Data;
import lombok.experimental.Accessors;
import org.springframework.data.annotation.Id;

@Data
@Accessors(chain = true)
public class AggregationResult {
    @Id
    private Map<String, Object> group;
    private Map<String, Object> data;
    /**
     * coordinate, 0 -> longitude, 1 -> latitude
     */
    private double[] geo;
}
