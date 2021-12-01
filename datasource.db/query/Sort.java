package com.wayz.lbi.industry.datasource.db.query;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Sort {
    private String field;
    private boolean isAsc;
}
