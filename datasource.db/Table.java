package com.wayz.lbi.industry.datasource.db;

import java.util.List;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Table {
    private String name;
    private List<Field> fields;
}
