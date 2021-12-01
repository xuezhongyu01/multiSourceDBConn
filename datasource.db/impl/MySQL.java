package com.wayz.lbi.industry.datasource.db.impl;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.wayz.lbi.common.utils.MapBuilder;
import com.wayz.lbi.industry.datasource.db.FieldType;
import com.wayz.lbi.industry.datasource.db.DB;
import com.wayz.lbi.industry.datasource.db.Field;
import com.wayz.lbi.industry.datasource.db.Table;
import com.wayz.lbi.industry.datasource.db.query.Aggregation;
import com.wayz.lbi.industry.datasource.db.query.Aggregator;
import com.wayz.lbi.industry.datasource.db.query.CondOperator;
import com.wayz.lbi.industry.datasource.db.query.Condition;
import com.wayz.lbi.industry.datasource.db.query.Select;
import com.wayz.lbi.industry.datasource.db.query.Sort;
import com.wayz.lbi.industry.datasource.db.query.Variable;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.data.util.Pair;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

@Slf4j
public class MySQL extends AbstractDB {

    private final Splitter commaSplitter = Splitter.on(',');
    private static final HashMap<String, JdbcTemplate> JDBC_TEMPLATE_CACHE = new HashMap<>();
    private JdbcTemplate jdbcTemplate;

    private static final Map<Integer, FieldType> SQL_TYPE_TO_FIELD_TYPE =
        MapBuilder.<Integer, FieldType>newBuilder()
            .put(Types.DATE, FieldType.DATETIME)
            .put(Types.TIME, FieldType.DATETIME)
            .put(Types.TIME_WITH_TIMEZONE, FieldType.DATETIME)
            .put(Types.TIMESTAMP, FieldType.DATETIME)
            .put(Types.TIME_WITH_TIMEZONE, FieldType.DATETIME)
            .put(Types.VARCHAR, FieldType.STRING)
            .put(Types.LONGNVARCHAR, FieldType.STRING)
            .put(Types.LONGVARCHAR, FieldType.STRING)
            .put(Types.NVARCHAR, FieldType.STRING)
            .put(Types.CHAR, FieldType.STRING)
            .put(Types.INTEGER, FieldType.NUMBER)
            .put(Types.BIGINT, FieldType.NUMBER)
            .put(Types.SMALLINT, FieldType.NUMBER)
            .put(Types.TINYINT, FieldType.NUMBER)
            .put(Types.DECIMAL, FieldType.NUMBER)
            .put(Types.DOUBLE, FieldType.NUMBER)
            .put(Types.FLOAT, FieldType.NUMBER)
            .put(Types.NUMERIC, FieldType.NUMBER)
            .put(Types.REAL, FieldType.NUMBER)
            .put(Types.BIT, FieldType.NUMBER)
            .build();


    interface Parameterizable {
        String op(String fieldId, Object value, String table);

        List<Object> val(Object value);
    }

    private static final EnumMap<CondOperator, Parameterizable> COND_PARAMETERIZ = new EnumMap<>(CondOperator.class);

    static {
        COND_PARAMETERIZ.put(CondOperator.EQ, new Parameterizable() {
            @Override
            public String op(String fieldId, Object value, String table) {
                if (Variable.MAX.equals(value)) {
                    // count = (SELECT MAX(count) FROM table)
                    return fieldId + " = (select MAX(" + fieldId + ") from `" + table + "`)";
                }
                return fieldId + "=?";
            }

            @Override
            public List<Object> val(Object value) {
                if (Variable.MAX.equals(value)) {
                    return Collections.emptyList();
                }
                return Lists.newArrayList(value);
            }
        });

        COND_PARAMETERIZ.put(CondOperator.IN, new Parameterizable() {
            @Override
            public String op(String fieldId, Object value, String table) {
                // fieldId in(?,?,?,?)
                StringBuilder sb = new StringBuilder();
                sb.append(fieldId).append(" in (");
                sb.append(Strings.repeat("?,", toList(value).size()));
                sb.delete(sb.length() - 1, sb.length()).append(")");
                return sb.toString();
            }

            @Override
            public List<Object> val(Object value) {
                return toList(value);
            }

            private List<Object> toList(Object value) {
                return (List<Object>) value;
            }
        });

        COND_PARAMETERIZ.put(CondOperator.LIKE, new Parameterizable() {
            @Override
            public String op(String fieldId, Object value, String table) {
                // fieldId like %query%
                StringBuilder sb = new StringBuilder();
                sb.append(fieldId).append(" like ? ");
                return sb.toString();
            }

            @Override
            public List<Object> val(Object value) {
                value = value == null ? "%" : "%" + value + "%";
                return Lists.newArrayList(value);
            }
        });

    }

    @Override
    public boolean connect(DB.ConnectInfo info) {

        Objects.requireNonNull(info);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(info.getHost()));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(info.getDbName()));

        try {
            getAndCacheJdbcTemplate(info).execute("select 1");
        } catch (Exception ex) {
            log.error("connect db failed", ex);
            return false;
        }
        return true;
    }

    private JdbcTemplate getAndCacheJdbcTemplate(DB.ConnectInfo info) {

        synchronized (JDBC_TEMPLATE_CACHE) {
            String key = buildKey(info);
            jdbcTemplate = JDBC_TEMPLATE_CACHE.get(key);
            if (Objects.isNull(jdbcTemplate)) {
                jdbcTemplate = new JdbcTemplate(
                    DataSourceBuilder.create()
                        .driverClassName("com.mysql.cj.jdbc.Driver")
                        .url(buildURL(info.getHost(), info.getPort(), info.getDbName()))
                        .username(info.getUserName())
                        .password(info.getPassword()).build());
                JDBC_TEMPLATE_CACHE.put(key, jdbcTemplate);
            }

            return jdbcTemplate;
        }
    }

    @Override
    String buildURL(String host, int port, String dbName) {
        return "jdbc:mysql://" + host + ':' + port + '/' + dbName
            + "?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    }

    @Override
    public List<String> showTables() {
        return jdbcTemplate.query(
            "show full tables where table_type = 'BASE TABLE'",
            (rs, i) -> rs.getString(1)
        );
    }

    @Override
    public Optional<Table> descTable(String table) {

        Preconditions.checkArgument(!Strings.isNullOrEmpty(table));
        Table ret = new Table().setName(table);

        try {
            jdbcTemplate.query(
                "select * from `" + table + "` where 1 < 0",
                rs -> {

                    ResultSetMetaData metaData = rs.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    List<Field> fields = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        FieldType t = SQL_TYPE_TO_FIELD_TYPE
                            .getOrDefault(metaData.getColumnType(i), FieldType.INVALID);
                        fields.add(new Field().setName(metaData.getColumnName(i))
                            .setFieldType(t));
                    }

                    ret.setFields(fields);
                    return null;
                }
            );
        } catch (BadSqlGrammarException ex) {
            log.error("table not exist:[" + table + "]", ex);
            return Optional.empty();
        }

        return Optional.of(ret);
    }

    @Override
    public List<Map<String, Object>> select(Select select) {

        Pair<String, Object[]> sqlAndArgs = buildSQLAndArgs(select);

        String sql = sqlAndArgs.getFirst();
        Object[] args = sqlAndArgs.getSecond();
        log.info("SQL:[{}]", sql);

        final List<Aggregation> aggs = select.getAggregations();
        Map<String, FieldType> nameToType =
            aggs.stream().anyMatch(a -> Aggregator.LIST == a.getAggregator()) ?
                prepareFieldType(select.getTable()) : Collections.emptyMap();

        return jdbcTemplate.query(sql, args, (rs, i) -> {

            Map<String, Object> b = new HashMap<>();

            for (Aggregation f : aggs) {
                String fieldId = f.getFieldId();
                Object v = rs.getObject(fieldId);
                if (Objects.isNull(v)) {
                    log.debug("{} with NULL", fieldId);
                    continue;
                }

                if (Aggregator.LIST == f.getAggregator()) {
                    v = processListResult(v, nameToType.get(fieldId));
                }

                b.put(fieldId, v instanceof BigDecimal ? ((BigDecimal) v).doubleValue() : v);
            }

            return b;
        }).stream().filter(r -> !r.isEmpty()).collect(Collectors.toList());
    }

    private Pair<String, Object[]> buildSQLAndArgs(Select select) {

        String table = select.getTable();
        List<Aggregation> fieldMaps = select.getAggregations();
        List<Condition> conditions = select.getConditions();
        List<Condition> orConditions = select.getOrConditions();
        List<String> groups = select.getGroups();
        List<Sort> sorts = select.getSorts();

        Preconditions.checkArgument(!Strings.isNullOrEmpty(table));
        Preconditions.checkArgument(!fieldMaps.isEmpty());
        Objects.requireNonNull(groups);
        Objects.requireNonNull(sorts);

        StringBuilder sb = new StringBuilder("select ")
            .append(fieldMaps.stream().map(this::getAggField).collect(Collectors.joining(",")))
            .append(" from ").append('`').append(table).append('`');

        Pair<String, Object[]> where = buildWhereAndArgs(conditions, orConditions, table);
        sb.append(where.getFirst());

        if (!groups.isEmpty()) {
            sb.append(" group by ").append(String.join(",", groups));
        }

        if (!sorts.isEmpty()) {
            sb.append(" order by ")
                .append(sorts.stream().map(s -> s.getField() + (s.isAsc() ? "" : " desc"))
                    .collect(Collectors.joining(",")));
        }

        if (select.getOffset() >= 0) {
            sb.append(" limit ").append(select.getOffset()).append(',').append(select.getLimit());
        }

        return Pair.of(sb.toString(), where.getSecond());
    }

    private Pair<String, Object[]> buildWhereAndArgs(List<Condition> filters, List<Condition> orConditions, String table) {

        if (CollectionUtils.isEmpty(filters) && CollectionUtils.isEmpty(orConditions)) {
            return Pair.of("", new Object[0]);
        }

        StringBuilder where = new StringBuilder(16).append(" where ");
        List<Object> args = new ArrayList<>(filters.size());
        String and = " and ";
        filters.forEach(c -> {
            where.append(opOf(c, table)).append(and);
            args.addAll(valueOf(c));
        });

        if (!orConditions.isEmpty()) {
            String or = " or ";
            where.append(" (");
            orConditions.forEach(c -> {
                where.append(opOf(c, table)).append(or);
                args.addAll(valueOf(c));
            });
            where.delete(where.length() - or.length(), where.length());
            where.append(") ");
        } else {
            where.delete(where.length() - and.length(), where.length());
        }

        return Pair.of(where.toString(), args.toArray());
    }

    private String opOf(Condition condition, String table) {
        Parameterizable pa = COND_PARAMETERIZ.get(condition.getOp());

        if (Objects.isNull(pa)) {
            throw new UnsupportedOperationException("mysql not support:" + condition.getOp().getName());
        }
        return pa.op(condition.getFieldId(), condition.getValue(), table);
    }

    private List<Object> valueOf(Condition condition) {
        Parameterizable pa = COND_PARAMETERIZ.get(condition.getOp());

        if (Objects.isNull(pa)) {
            throw new UnsupportedOperationException("mysql not support:" + condition.getOp().getName());
        }

        return pa.val(condition.getValue());
    }

    private String getAggField(Aggregation agg) {

        String fieldId = agg.getFieldId();
        switch (agg.getAggregator()) {
            case ID:
                return fieldId;
            case AVG:
                return "avg(" + fieldId + ") " + fieldId;
            case MAX:
                return "max(" + fieldId + ") " + fieldId;
            case MIN:
                return "min(" + fieldId + ") " + fieldId;
            case COUNT:
                return "count(" + fieldId + ") " + fieldId;
            case SUM:
                return "sum(" + fieldId + ") " + fieldId;
            case LIST:
                return "group_concat(ifnull(" + fieldId + ",'null')) " + fieldId;
            case DISTINCT:
                return "distinct(" + fieldId + ") " + fieldId;
            default:
                throw new UnsupportedOperationException("MYSQL unsupported, agg:" + agg);

        }
    }

    private Map<String, FieldType> prepareFieldType(String table) {
        return descTable(table).orElseThrow(
            () -> new RuntimeException("BUG: desc table failed")
        ).getFields().stream().collect(Collectors.toMap(Field::getName, Field::getFieldType));
    }

    private Object processListResult(Object v, FieldType t) {

        return commaSplitter.splitToList((String) v).stream().map(s -> parse(s, t))
            .collect(Collectors.toList());
    }

    private Object parse(String v, FieldType t) {

        if ("null".equals(v)) {
            return t.defaultValue();
        }

        switch (t) {
            case STRING:
            case DATETIME:
                return v;
            case NUMBER:
                return Double.parseDouble(v);
        }

        throw new RuntimeException("IMPOSSIBLE HERE");
    }

    @Override
    public long total(String table, List<Condition> conditions) {

        StringBuilder sb = new StringBuilder("select count(1) as " + TOTAL_FIELD + " from `").append(table).append("`");
        Object[] args = new Object[0];
        if (Objects.nonNull(conditions) && !conditions.isEmpty()) {
            Pair<String, Object[]> where = buildWhereAndArgs(conditions, Collections.EMPTY_LIST, table);
            sb.append(where.getFirst());
            args = where.getSecond();
        }

        Long r = jdbcTemplate.queryForObject(
            sb.toString(),
            args,
            (rs, i) -> rs.getLong(TOTAL_FIELD)
        );
        return Objects.nonNull(r) ? r : 0L;
    }

    @Override
    public String queryStat(Select select) {
        Objects.requireNonNull(select);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(select.getTable()));

        return selectSQL(select) + fromSQL(select) + whereSQL(select) + groupBySQL(select)
            + orderBySQL(select) + limitSQL(select);
    }

    @Override
    public void batchInsert(String table, List<Map<String, Object>> data) {
        if (CollectionUtils.isEmpty(data))
            return;

        String sql = buildInsertSql(table, data.get(0).keySet());
        jdbcTemplate.batchUpdate(sql, data.stream().map(map -> map.values().toArray()).collect(Collectors.toList()));
    }

    protected String buildInsertSql(String table, Set<String> fieldNames) {
        StringBuffer sb = new StringBuffer();
        sb.append("REPLACE INTO ") // 防止重复key抛出异常
            .append(table)
            .append(fieldNames.stream().collect(Collectors.joining("`,`", "(`", "`)")))
            .append(" VALUES")
            .append(Stream.generate(() -> "?").limit(fieldNames.size()).collect(Collectors.joining(",", "(", ")")));

        return sb.toString();
    }

    private String selectSQL(Select select) {

        return "select " + wrap(select.getAggregations()).stream()
            .map(this::getAggField)
            .collect(Collectors.joining(","));
    }

    private String fromSQL(Select select) {
        return " from " + select.getTable();
    }

    private String whereSQL(Select select) {

        List<Condition> conds = wrap(select.getConditions());
        if (conds.isEmpty()) {
            return "";
        }

        return " where " + wrap(select.getConditions())
            .stream()
            .map(c -> c.getFieldId() + c.getOp().getName() + c.getValue().toString())
            .collect(Collectors.joining(" and "));
    }

    private String groupBySQL(Select select) {
        return wrap(select.getGroups()).isEmpty() ?
            "" :
            (" group by " + String.join(",", select.getGroups()));
    }

    private String orderBySQL(Select select) {

        return wrap(select.getSorts()).isEmpty() ?
            "" :
            (" order by " + select.getSorts().stream()
                .map(s -> s.getField() + (s.isAsc() ? "" : " desc"))
                .collect(Collectors.joining(",")));
    }

    private String limitSQL(Select select) {

        return " limit " + select.getOffset() + "," + select.getLimit();
    }

    private <T> List<T> wrap(List<T> l) {
        return Objects.isNull(l) ? Collections.emptyList() : l;
    }
}
