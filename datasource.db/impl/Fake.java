package com.wayz.lbi.industry.datasource.db.impl;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import com.wayz.lbi.industry.datasource.db.DB;
import com.wayz.lbi.industry.datasource.db.Table;
import com.wayz.lbi.industry.datasource.db.query.Condition;
import com.wayz.lbi.industry.datasource.db.query.Select;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * Fake db used for testing
 */
@Data
@Accessors(chain = true)
public class Fake extends AbstractDB {

    private static final Fake INSTANCE = new Fake();

    public static Fake getInstance() {
        return INSTANCE;
    }

    private String buildURLResult;
    private boolean connectResult;
    private List<String> showTableResult;
    private List<Map<String, Object>> selectResult;
    private List<Map<String, Object>> selectFilterResult;
    private Map<String, Object> filters;
    private List<Map<String, Object>> selectSelectResult;
    private Table descTableResult;
    private long totalResult;
    private Select select;
    private RuntimeException exception;
    private Supplier<RuntimeException> selectExpGenerator;

    private boolean buildURLCalled;
    private boolean connectCalled;

    @Override
    String buildURL(String host, int port, String dbName) {
        buildURLCalled = true;
        return buildURLResult;
    }

    @Override
    public boolean connect(DB.ConnectInfo info) {
        connectCalled = true;
        return connectResult;
    }

    @Override
    public List<String> showTables() {
        return showTableResult;
    }

    @Override
    public Optional<Table> descTable(String table) {
        return Optional.ofNullable(descTableResult);
    }

    @Override
    public List<Map<String, Object>> select(Select select) {
        this.select = select;
        return selectSelectResult;
    }

    @Override
    public long total(String table, List<Condition> conditions) {
        return selectSelectResult.size();
    }

    @Override
    public String queryStat(Select s) {
        return "fake select";
    }

    @Override
    public void batchInsert(String table, List<Map<String, Object>> data) {

    }
}
