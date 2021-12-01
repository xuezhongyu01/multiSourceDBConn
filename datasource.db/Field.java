package com.wayz.lbi.industry.datasource.db;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Field {
    private String name;
    private FieldType fieldType;
}
