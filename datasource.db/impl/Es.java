package com.wayz.lbi.industry.datasource.db.impl;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.wayz.lbi.common.utils.DateUtils;
import com.wayz.lbi.common.utils.MapBuilder;
import com.wayz.lbi.industry.datasource.db.DB;
import com.wayz.lbi.industry.datasource.db.Field;
import com.wayz.lbi.industry.datasource.db.FieldType;
import com.wayz.lbi.industry.datasource.db.Table;
import com.wayz.lbi.industry.datasource.db.query.Aggregation;
import com.wayz.lbi.industry.datasource.db.query.Aggregator;
import com.wayz.lbi.industry.datasource.db.query.Condition;
import com.wayz.lbi.industry.datasource.db.query.Select;
import com.wayz.lbi.industry.datasource.db.query.Sort;
import com.wayz.lbi.industry.datasource.db.query.Variable;

import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.ParsedComposite;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.DoubleTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.ParsedAvg;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.ParsedCardinality;
import org.elasticsearch.search.aggregations.metrics.geocentroid.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.geocentroid.ParsedGeoCentroid;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.ParsedMax;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.ParsedMin;
import org.elasticsearch.search.aggregations.metrics.sum.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ParsedValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketsort.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 * db name <-> index db table <-> type
 */
@Slf4j
public class Es extends AbstractDB {

    static class DateFormat {
        static String yyMMdd = "yyMMdd";
        static String yyMM = "yyMM";
    }

    private static final String TYPE_NAME = "_type";
    private static final String GEO_TYPE_NAME = "geo_point";
    private static final String PROPERTIES = "properties";
    private static final String DATE_TYPE_NAME = "date";
    static final String TEXT_TYPE_NAME = "text";

    private static final String LAT_SUFFIX = ".lat";
    private static final String LNG_SUFFIX = ".lon";

    private static final Map<String, FieldType> TYPE_TO_FIELD_TYPE = ImmutableMap.<String, FieldType>builder()
        .put("long", FieldType.NUMBER)
        .put("integer", FieldType.NUMBER)
        .put("short", FieldType.NUMBER)
        .put("byte", FieldType.NUMBER)
        .put("double", FieldType.NUMBER)
        .put("float", FieldType.NUMBER)
        .put("half_float", FieldType.NUMBER)
        .put("scaled_float", FieldType.NUMBER)
        .put(TEXT_TYPE_NAME, FieldType.STRING)
        .put("keyword", FieldType.STRING)
        .put(GEO_TYPE_NAME, FieldType.NUMBER)
        .put(DATE_TYPE_NAME, FieldType.DATETIME)
        .put("boolean", FieldType.NUMBER)
        .build();


    private static final LoadingCache<DB.ConnectInfo, RestHighLevelClient> CLIENT_CACHE =
        Caffeine.newBuilder()
            .maximumSize(10_000)
            .expireAfterAccess(Duration.ofMillis(RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS - 1000L))
            .recordStats()
            .removalListener((DB.ConnectInfo k, RestHighLevelClient v, RemovalCause c) -> {
                try {
                    log.error("remove the client, key:[{}], v:[{}], cause:[{}]", k, v, c);

                    if (Objects.nonNull(v)) {
                        v.close();
                    }
                } catch (IOException e) {
                    log.error("removal listener:{}", k);
                }
            })
            .build(c -> new RestHighLevelClient(
                RestClient.builder(new HttpHost(c.getHost(), c.getPort(), "http"))
            ));

    static {
        CacheMetricsCollector cacheMetrics = new CacheMetricsCollector().register();
        cacheMetrics.addCache("es-client-cache", CLIENT_CACHE);
    }

    private DB.ConnectInfo info;

    @Override
    public boolean connect(DB.ConnectInfo info) {

        copyConnInfo(info);

        RestHighLevelClient client = getAndCacheClient();
        try {
            tryFixDbNameSinceAlias(client, info);

            GetMappingsRequest req = new GetMappingsRequest().indices(this.info.getDbName());
            client.indices().getMapping(req, RequestOptions.DEFAULT);
            return true;
        } catch (Exception e) {
            log.error("test failed", e);
            close(client);
        }

        return false;
    }

    private void copyConnInfo(DB.ConnectInfo info) {
        this.info = new DB.ConnectInfo()
            .setDbName(info.getDbName())
            .setHost(info.getHost())
            .setPort(info.getPort())
            .setUserName(info.getUserName())
            .setPassword(info.getPassword());
    }

    private void tryFixDbNameSinceAlias(RestHighLevelClient client, DB.ConnectInfo info) throws IOException {

        String dbName = info.getDbName();

        GetAliasesResponse aliasesResponse = client.indices().getAlias(
            new GetAliasesRequest().aliases(dbName), RequestOptions.DEFAULT
        );

        Map<String, Set<AliasMetaData>> aliases = aliasesResponse.getAliases();

        if (Objects.isNull(aliases)) {
            return;
        }

        for (Map.Entry<String, Set<AliasMetaData>> e : aliases.entrySet()) {
            String index = e.getKey();
            Optional<AliasMetaData> aOpt = e.getValue().stream()
                .filter(a -> a.getAlias().equals(dbName)).findFirst();

            if (aOpt.isPresent()) {
                this.info.setDbName(index);
                break;
            }
        }
    }

    @Override
    public List<String> showTables() {

        RestHighLevelClient client = getAndCacheClient();

        try {
            GetMappingsResponse resp = client.indices().getMapping(
                new GetMappingsRequest().indices(info.getDbName()), RequestOptions.DEFAULT
            );
            ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> map = resp
                .mappings();

            if (Objects.isNull(map)) {
                return Collections.emptyList();
            }

            ImmutableOpenMap<String, MappingMetaData> typeMetaData = map.get(info.getDbName());
            if (Objects.isNull(typeMetaData)) {
                return Collections.emptyList();
            }

            return Lists.newArrayList(typeMetaData.keysIt());
        } catch (Exception e) {
            close(client);
            log.error("show table failed", e);
        }

        return Collections.emptyList();
    }

    @Override
    public Optional<Table> descTable(String table) {

        Map<String, EsField> nameToField = getFieldNameAndType(table);
        if (nameToField == null) {
            return Optional.empty();
        }

        return Optional.of(new Table().setName(table).setFields(buildFields(nameToField)));
    }

    private Map<String, EsField> getFieldNameAndType(String table) {

        RestHighLevelClient client = getAndCacheClient();

        GetMappingsResponse resp;
        try {
            resp = client.indices().getMapping(
                new GetMappingsRequest().indices(info.getDbName()), RequestOptions.DEFAULT
            );
        } catch (IOException e) {
            log.error("request indice error", e);
            close(client);
            return null;
        }

        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> map = resp.mappings();

        if (Objects.isNull(map)) {
            return null;
        }

        ImmutableOpenMap<String, MappingMetaData> typeMetaData = map.get(info.getDbName());
        if (Objects.isNull(typeMetaData)) {
            return null;
        }

        MappingMetaData metaData = typeMetaData.get(table);
        if (Objects.isNull(metaData)) {
            return null;
        }

        return ((Map<String, Object>) metaData.sourceAsMap().get("properties")).entrySet().stream()
            .map(e -> EsField.of(e.getKey(), (Map<String, Object>) e.getValue()))
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(EsField::getName, f -> f));
    }

    private List<Field> buildFields(Map<String, EsField> nameToField) {

        List<Field> fields = new ArrayList<>(nameToField.size() + 1);

        nameToField.forEach((n, f) -> {

            String type = f.getType();
            FieldType fieldType = TYPE_TO_FIELD_TYPE.getOrDefault(type, FieldType.INVALID);
            fields.add(new Field().setName(n).setFieldType(fieldType));
        });

        return fields;
    }


    @Override
    public List<Map<String, Object>> select(Select select) {

        Map<String, EsField> nameToField = getFieldNameAndType(select.getTable());
        log.info("SELECT:[{}], fields:[{}]", select, nameToField);
        Preconditions.checkState(
            nameToField != null, "no field type info"
        );
        // store the _type, used by the total
        nameToField.put(TYPE_NAME, new EsField().setName(TYPE_NAME).setType("keyword"));

        List<Map<String, Object>> ret = selectRaw(select, nameToField);
        log.info("SELECT result:[{}]", ret);
        ret.forEach(m -> {
            fixDateResult(m, nameToField, select.getAggregations());
            fixGeoPointResult(m, nameToField);
        });
        log.info("SELECT FIXED result:[{}]", ret);

        return ret;
    }

    private List<Map<String, Object>> selectRaw(Select select, Map<String, EsField> nameToField) {

        Objects.requireNonNull(select);
        List<Aggregation> aggs = select.getAggregations();
        Objects.requireNonNull(aggs);
        Preconditions.checkArgument(!aggs.isEmpty());
        Preconditions.checkArgument(!Strings.isNullOrEmpty(select.getTable()));

        SearchSourceBuilder ssBuilder = new SearchSourceBuilder();
        // merge geo type
        aggs = tryMergeGeoPointField(aggs, nameToField);
        boolean hasAggs = !select.getGroups().isEmpty() ||
            aggs.stream().anyMatch(a -> a.getAggregator() != Aggregator.ID);

        if (hasAggs) {
            // not return hits
            ssBuilder.size(0);
            if (select.getGroups().isEmpty()) {
                buildAggs(aggs, ssBuilder, nameToField, select.getLimit(), select.getSorts());
            } else {
                ssBuilder.aggregation(buildAggs(
                    select.getGroups(),
                    aggs.stream().filter(a -> Aggregator.ID != a.getAggregator())
                        .collect(Collectors.toList()),
                    select.getSorts(),
                    select.getLimit(),
                    nameToField
                ));
            }
        }

        log.info("SELECT builder:[{}]", ssBuilder);
        RestHighLevelClient client = getAndCacheClient();
        try {
            SearchResponse resp = client.search(
                new SearchRequest(new String[] {info.getDbName()}, ssBuilder),
                RequestOptions.DEFAULT
            );

            List<Map<String, Object>> ret = parseResponse(resp);
            ret.forEach(m -> fixTextAgg(m, nameToField));

            return ret;
        } catch (Exception e) {
            close(client);
            log.error("search failed", e);
            throw new IllegalStateException("search failed:" + info.displayString());
        }
    }

    private Collection<?> getValue(
        String table, String fieldName, Map<String, EsField> nameToField, Collection<?> value
    ) {
        return value.stream().map(v -> getValue(table, fieldName, nameToField, v))
            .collect(Collectors.toList());
    }

    private Object getValue(
        String table, String fieldName, Map<String, EsField> nameToField, Object value
    ) {

        EsField field = nameToField.get(fieldName);
        if (Variable.MAX.equals(value)) {
            return max(
                table, field.getName(), nameToField, TYPE_TO_FIELD_TYPE.get(field.getType()).defaultValue()
            );
        }

        return transferParams(value, field);
    }

    private static Object transferParams(Object o, EsField field) {

        if (!DATE_TYPE_NAME.equals(field.getType())) {
            return o;
        }

        String format = field.getFormat();
        if (Strings.isNullOrEmpty(format) || DateFormat.yyMMdd.equals(format)) {
            // .eg: 2020-02-01 00:00:00
            if (o instanceof String) {
                String s = (String) o;
                return Integer.parseInt(s.substring(2, 4) + s.substring(5, 7) + s.substring(8, 10));
            }

            if (o instanceof LocalDateTime) {
                return formatDateParam((LocalDateTime) o, DateFormat.yyMMdd);
            }

            if (o instanceof Date) {
                return formatDateParam(((Date) o).getTime(), DateFormat.yyMMdd);
            }

            return o;
        } else {
            throw new UnsupportedOperationException("unsupported format:" + format);
        }
    }

    private Object max(String table, String fieldId, Map<String, EsField> nameToField,
        Object defaultValue) {

        Select s = new Select().setTable(table).addAggregation(
            new Aggregation().setAggregator(Aggregator.MAX).setFieldId(fieldId)
        );

        List<Map<String, Object>> r = selectRaw(s, nameToField);

        if (isNullOrEmpty(r)) {
            return defaultValue;
        }

        Object o = r.get(0).get(fieldId);

        EsField esField = nameToField.get(fieldId);
        if (!DATE_TYPE_NAME.equals(esField.getType())) {
            return o;
        }

        if (!(o instanceof Number)) {
            return o;
        }

        return formatDateParam(((Number) o).longValue(), esField.getFormat());
    }

    private static List<Aggregation> tryMergeGeoPointField(List<Aggregation> aggs,
        Map<String, EsField> nameToField) {

        if (nameToField.entrySet().stream().noneMatch(
            e -> GEO_TYPE_NAME.equals(e.getValue().getType()) && aggs.stream().anyMatch(
                a -> {
                    String fieldId = a.getFieldId();
                    String esFieldId = e.getKey();
                    return fieldId.equals(esFieldId + LAT_SUFFIX) || fieldId
                        .equals(esFieldId + LNG_SUFFIX);
                }
            ))) {
            return aggs;
        }

        List<Aggregation> mergedAggs = new ArrayList<>(aggs.size());
        List<String> merged = new ArrayList<>(2);

        aggs.forEach(a -> {
            String fieldId = a.getFieldId();
            if (isGeoPoint(fieldId, nameToField, LAT_SUFFIX) ||
                isGeoPoint(fieldId, nameToField, LNG_SUFFIX)) {

                // since the length of the LAT_SUFFIX&LNG_SUFFIX is equal
                String fieldInDb = fieldId.substring(0, fieldId.length() - LAT_SUFFIX.length());

                if (!merged.contains(fieldInDb)) {
                    mergedAggs.add(
                        new Aggregation().setFieldId(fieldInDb).setAggregator(a.getAggregator()));
                    merged.add(fieldInDb);
                }
            } else {
                mergedAggs.add(a);
            }
        });

        return mergedAggs;
    }

    private static boolean isGeoPoint(String fieldId, Map<String, EsField> nameToField,
        String suffix) {

        return fieldId.endsWith(suffix) && GEO_TYPE_NAME.equals(
            nameToField.get(fieldId.substring(0, fieldId.length() - suffix.length())).getType()
        );
    }

    private static List<Condition> tryConvertGeoPointField(List<Condition> conditions) {
        // TODO(zenk), support geo-point condition
        return conditions;
    }

    private static void buildAggs(List<Aggregation> aggs, SearchSourceBuilder ssBuilder,
        Map<String, EsField> nameToField, int size, List<Sort> sorts) {

        aggs.forEach(a -> ssBuilder.aggregation(
            buildAggBuilder(a, nameToField.get(a.getFieldId()), size,
                sorts.stream()
                    .filter(s -> a.getFieldId().equals(s.getField()))
                    .findFirst()
                    .map(Sort::isAsc).orElse(false)
            )
        ));
    }

    private static AggregationBuilder buildAggs(List<String> groups, List<Aggregation> aggs,
        List<Sort> sorts, int size, Map<String, EsField> nameToField) {

        int gz = groups.size();
        Preconditions.checkArgument(gz > 0);

        if (gz == 1) {
            return buildOneGroupAgg(groups, aggs, sorts, size, nameToField);
        }

        return buildMultiGroupAggs(groups, aggs, sorts, size, nameToField);
    }

    private static AggregationBuilder buildMultiGroupAggs(
        List<String> groups, List<Aggregation> aggs, List<Sort> sorts, int size,
        Map<String, EsField> nameToField
    ) {
        CompositeAggregationBuilder compositeAggregationBuilder =
            new CompositeAggregationBuilder("cs", builderCompositeAgg(
                groups, sorts, nameToField
            ));
        aggs.forEach(a -> compositeAggregationBuilder.subAggregation(buildAggBuilder(
            a, nameToField.get(a.getFieldId())
        )));

        if (sorts.stream().anyMatch(
            s -> aggs.stream().anyMatch(a -> a.getFieldId().equals(s.getField()))
        )) {
            compositeAggregationBuilder.subAggregation(buildAggSort(aggs, sorts));
        }

        return compositeAggregationBuilder.size(size);
    }

    private static AggregationBuilder buildOneGroupAgg(
        List<String> groups, List<Aggregation> aggs, List<Sort> sorts, int size,
        Map<String, EsField> nameToField
    ) {

        String field = groups.get(0);
        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms(field)
            .field(nameToField.get(field).getAggFieldName());
        sorts.stream().filter(s -> s.getField().equals(field)).findFirst().ifPresent(
            s -> termsAggregationBuilder.order(BucketOrder.key(s.isAsc()))
        );
        aggs.forEach(a -> termsAggregationBuilder.subAggregation(buildAggBuilder(
            a, nameToField.get(a.getFieldId())
        )));

        if (sorts.stream().anyMatch(
            s -> aggs.stream().anyMatch(a -> a.getFieldId().equals(s.getField()))
        )) {
            termsAggregationBuilder.subAggregation(buildAggSort(aggs, sorts));
        }
        return termsAggregationBuilder.size(size);
    }

    private static List<CompositeValuesSourceBuilder<?>> builderCompositeAgg(List<String> groups,
        List<Sort> sorts, Map<String, EsField> nameToField) {

        int size = groups.size();
        Preconditions.checkArgument(size > 1);

        List<CompositeValuesSourceBuilder<?>> sourceBuilders = new ArrayList<>(size);
        for (String g : groups) {

            TermsValuesSourceBuilder sourceBuilder = new TermsValuesSourceBuilder(g)
                .field(nameToField.get(g).getAggFieldName());

            sorts.stream().filter(s -> s.getField().equals(g)).findFirst().ifPresent(
                s -> sourceBuilder.order(s.isAsc() ? SortOrder.ASC : SortOrder.DESC)
            );

            sourceBuilders.add(sourceBuilder);
        }

        return sourceBuilders;
    }

    private static BucketSortPipelineAggregationBuilder buildAggSort(List<Aggregation> aggs,
        List<Sort> sorts) {

        return new BucketSortPipelineAggregationBuilder(
            "aggs_sort",
            sorts.stream()
                .filter(s -> aggs.stream().anyMatch(a -> s.getField().equals(a.getFieldId())))
                .map(s -> SortBuilders.fieldSort(s.getField())
                    .order(s.isAsc() ? SortOrder.ASC : SortOrder.DESC))
                .collect(Collectors.toList())
        );
    }

    private static AggregationBuilder buildAggBuilder(Aggregation agg, EsField field) {
        return buildAggBuilder(agg, field, 0, false);
    }

    private static AggregationBuilder buildAggBuilder(Aggregation agg, EsField field, int size,
        boolean distinctAscOrder) {

        String fieldId = field.getAggFieldName();
        switch (agg.getAggregator()) {
            case COUNT:
                return AggregationBuilders.count(fieldId).field(fieldId);
            case AVG:
                if (GEO_TYPE_NAME.equals(field.getType())) {
                    return AggregationBuilders.geoCentroid(fieldId).field(fieldId);
                }
                return AggregationBuilders.avg(fieldId).field(fieldId);
            case MAX:
                return AggregationBuilders.max(fieldId).field(fieldId);
            case MIN:
                return AggregationBuilders.min(fieldId).field(fieldId);
            case SUM:
                return AggregationBuilders.sum(fieldId).field(fieldId);
            case DISTINCT:
                Preconditions.checkArgument(size > 0, "distinct from the multi-agg");
                return AggregationBuilders.terms(fieldId).field(fieldId).size(size).order(BucketOrder.key(distinctAscOrder));
            case LIST:
            case ID:
            default:
                throw new UnsupportedOperationException("unsupported now:" + agg.getAggregator());
        }
    }

    /**
     * only support the hits or the aggregations
     *
     * @param resp search response
     * @return value list
     */
    private static List<Map<String, Object>> parseResponse(SearchResponse resp) {

        log.info("SELECT resp:[{}]", resp);

        SearchHits hits = resp.getHits();
        if (Objects.nonNull(hits) && hits.iterator().hasNext()) {
            return parseHit(resp.getHits());
        }

        Aggregations aggs = resp.getAggregations();
        if (Objects.nonNull(aggs)) {
            return parseAgg(resp.getAggregations());
        }

        return Collections.emptyList();
    }

    private static List<Map<String, Object>> parseHit(SearchHits searchHits) {

        return Streams.stream(searchHits)
            .map(h -> flatMap(h.getSourceAsMap()))
            .collect(Collectors.toList());
    }

    private static Map<String, Object> flatMap(Map<String, Object> m) {

        return m.entrySet().stream().map(e -> {
            Object v = e.getValue();
            if (Objects.isNull(v)) {
                return Collections.<Pair<String, Object>>emptyList();
            }

            String k = e.getKey();
            if (v instanceof Map) {
                return mapToPairsWithPrefixKey(flatMap((Map<String, Object>) v), k);
            } else if (v instanceof Collection) {
                return mapToPairsWithPrefixKey(
                    flatCollectionMap((Collection<Map<String, Object>>) v), k);
            }
            return ImmutableList.of(Pair.of(k, v));
        })
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    }

    private static Map<String, Object> flatCollectionMap(Collection<Map<String, Object>> ms) {

        return ms.stream().reduce(new HashMap<>(), (r, m) -> {
            m.keySet().forEach(k -> {
                    Object o = r.get(k);
                    if (Objects.isNull(o)) {
                        r.put(k, Objects.toString(m.get(k)));
                    } else {
                        r.put(k, o.toString() + "," + m.get(k));
                    }
                }
            );
            return r;
        });
    }

    private static List<Pair<String, Object>> mapToPairsWithPrefixKey(Map<String, Object> m,
        String k) {
        return m.entrySet().stream()
            .map(e -> Pair.of(k + "." + e.getKey(), e.getValue()))
            .collect(Collectors.toList());
    }

    /**
     * TODO(zenk), support other date format
     *
     * @param o
     * @param format
     * @return
     */
    private static String parseDate(Object o, String format) {

        long dt;

        if (Objects.isNull(o)) {
            return "0000-00-00 00:00:00";
        } else if (o instanceof Number) {
            dt = ((Number) o).longValue();
        } else if (o instanceof String) {
            dt = Long.parseLong((String) o);
        } else {
            throw new IllegalArgumentException("unknown value type:" + o.getClass().getSimpleName());
        }

        if (Strings.isNullOrEmpty(format)) {
            return yyMMdd(dt);
        }

        if (DateFormat.yyMMdd.equals(format)) {
            if (dt < 1000000) {
                return yyMMdd(dt);
            }
            LocalDateTime localDateTime = DateUtils.fromUnixTimeMillis(dt);
            return DateUtils.formatLocalDate(localDateTime.toLocalDate()) + " 00:00:00";
        }

        if (DateFormat.yyMM.equals(format)) {
            if (dt < 10000) {
                return yyMM(dt);
            }
            LocalDateTime localDateTime = DateUtils.fromUnixTimeMillis(dt);
            return DateUtils.formatLocalDate(localDateTime.toLocalDate()) + " 00:00:00";
        }

        throw new UnsupportedOperationException("not supported:" + format);
    }

    private static String yyMMdd(long dt) {
        int year = Year.now().getValue();
        return String.format(
            "%4d-%02d-%02d",
            year - year % 100 + dt / 10000, dt % 10000 / 100, dt % 100
        ) + " 00:00:00";
    }

    private static String yyMM(long dt) {
        int year = Year.now().getValue();
        return String.format("%4d-%02d-01", year - year % 100 + dt / 100, dt % 100) + " 00:00:00";
    }

    /**
     * convert the unix time (millisecond) to the date value according the format stored in the es
     *
     * @param dt     unix time (millisecond unit)
     * @param format date format
     * @return date
     */
    private static long formatDateParam(long dt, String format) {

        if (Strings.isNullOrEmpty(format)) {
            return dt;
        }

        LocalDateTime localDateTime = DateUtils.fromUnixTimeMillis(dt);
        return formatDateParam(localDateTime, format);
    }

    private static long formatDateParam(LocalDateTime dt, String format) {

        if (DateFormat.yyMMdd.equals(format)) {
            return 10000L * (dt.getYear() % 100) + dt.getMonthValue() * 100 + dt.getDayOfMonth();
        }
        throw new UnsupportedOperationException("not supported:" + format);
    }

    private static List<Map<String, Object>> parseAgg(Aggregations aggs) {

        return Streams.stream(aggs)
            .map(Es::parseAgg)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private static List<Map<String, Object>> parseAgg(
        org.elasticsearch.search.aggregations.Aggregation agg) {

        String type = agg.getType();

        if (CompositeAggregationBuilder.NAME.equals(type)) {
            return parseComposite((ParsedComposite) agg);
        }

        if (StringTerms.NAME.equals(type) ||
            LongTerms.NAME.equals(type) ||
            DoubleTerms.NAME.equals(type)
        ) {
            return parseTerms((ParsedTerms) agg);
        }

        if (!metricAggParsers.containsKey(type)) {
            log.error("not contain:[{}]", type);
            throw new IllegalStateException("not supported of type:" + type);
        }

        return Lists.newArrayList(MapBuilder.<String, Object>newBuilder()
            .put(agg.getName(), metricAggParsers.get(type).apply(agg))
            .build());
    }

    private static Map<String, Function<org.elasticsearch.search.aggregations.Aggregation, Object>> metricAggParsers =
        MapBuilder.<String, Function<org.elasticsearch.search.aggregations.Aggregation, Object>>newBuilder()
            .put(AvgAggregationBuilder.NAME, a -> filterInfinity(((ParsedAvg) a).getValue(), 0))
            .put(MaxAggregationBuilder.NAME, a -> filterInfinity(((ParsedMax) a).getValue(), Double.MAX_VALUE))
            .put(MinAggregationBuilder.NAME, a -> filterInfinity(((ParsedMin) a).getValue(), Double.MIN_VALUE))
            .put(SumAggregationBuilder.NAME, a -> filterInfinity(((ParsedSum) a).getValue(), Double.MAX_VALUE))
            .put(ValueCountAggregationBuilder.NAME, a -> ((ParsedValueCount) a).getValue())
            .put(CardinalityAggregationBuilder.NAME, a -> ((ParsedCardinality) a).getValue())
            .put(GeoCentroidAggregationBuilder.NAME, a -> ((ParsedGeoCentroid) a).centroid())
            .build();

    private static double filterInfinity(double v, double defaultValue) {
        if (Double.isInfinite(v)) {
            return defaultValue;
        }

        return v;
    }

    private static List<Map<String, Object>> parseComposite(ParsedComposite pc) {

        return pc.getBuckets().stream().map(b -> {
            Map<String, Object> r = new HashMap<>(b.getKey());
            List<Map<String, Object>> sub = parseAgg(b.getAggregations());
            sub.forEach(r::putAll);
            return r;
        }).collect(Collectors.toList());
    }

    private static List<Map<String, Object>> parseTerms(ParsedTerms lt) {

        List<? extends Terms.Bucket> buckets = lt.getBuckets();
        return buckets.stream().map(b -> {
            Map<String, Object> r = new HashMap<>();
            r.put(lt.getName(), b.getKey());
            parseAgg(b.getAggregations()).forEach(r::putAll);
            return r;
        }).collect(Collectors.toList());
    }

    private static void fixTextAgg(Map<String, Object> m, Map<String, EsField> nameToField) {

        m.keySet().stream().filter(k -> k.indexOf('.') >= 0)
            .collect(Collectors.toList())
            .forEach(k -> {
                String name = k.substring(0, k.indexOf('.'));
                EsField f = nameToField.get(name);
                if (Objects.nonNull(f) && f.isText()) {
                    m.put(name, m.remove(k));
                }
            });
    }

    private static void fixGeoPointResult(Map<String, Object> m, Map<String, EsField> nameToField) {

        m.keySet().stream()
            .filter(k -> GEO_TYPE_NAME.equals(nameToField.get(k).getType()))
            .collect(Collectors.toList())
            .forEach(n -> updateGeo(m, n));
    }

    private static void updateGeo(Map<String, Object> m, String name) {

        Object v = m.remove(name);

        Double lat = null;
        Double lng = null;

        if (Objects.isNull(v)) {
            log.warn("lat&lng is null");
        } else if (v instanceof GeoPoint) {
            lat = ((GeoPoint) v).getLat();
            lng = ((GeoPoint) v).getLon();
        } else if (v instanceof String) {
            int i = v.toString().indexOf(',');
            lat = Double.parseDouble(v.toString().substring(0, i).trim());
            lng = Double.parseDouble(v.toString().substring(i + 1).trim());
        } else {
            throw new IllegalStateException(
                "BUG: unsupported geo value:" + v.toString() + ", type:" + v.getClass().getCanonicalName());
        }

        m.put(name + LAT_SUFFIX, lat);
        m.put(name + LNG_SUFFIX, lng);
    }

    private static void fixDateResult(Map<String, Object> m, Map<String, EsField> nameToField,
        List<Aggregation> aggregations) {

        m.forEach((k, v) -> {
            EsField f = nameToField.get(k);
            if (Objects.isNull(f)) {
                throw new IllegalStateException(
                    "bug response field not in the fields. field:" + k + ", value:" + v
                );
            }

            if (!DATE_TYPE_NAME.equals(f.getType())) {
                return;
            }
            if (!isGenDateResult(k, aggregations)) {
                return;
            }
            m.put(k, parseDate(m.get(k), f.getFormat()));
        });
    }

    private static boolean isGenDateResult(String dateFieldName, List<Aggregation> aggregations) {
        return aggregations.stream().filter(a -> dateFieldName.equals(a.getFieldId())).findFirst()
            .map(a -> {
                Aggregator aggregator = a.getAggregator();
                return aggregator == Aggregator.ID
                    || aggregator == Aggregator.MAX || aggregator == Aggregator.MIN
                    || aggregator == Aggregator.DISTINCT || aggregator == Aggregator.LIST;
            })
            .orElse(false);
    }

    @Override
    public long total(String table, List<Condition> conditions) {
        Select s = new Select().setTable(table)
            .setAggregations(Collections.singletonList(
                new Aggregation().setAggregator(Aggregator.COUNT).setFieldId(TYPE_NAME)
            ));

        if (Objects.nonNull(conditions) && !conditions.isEmpty()) {
            s.setConditions(conditions);
        }

        List<Map<String, Object>> r = select(s);
        return isNullOrEmpty(r) ? 0L : (long) r.get(0).get(TYPE_NAME);
    }

    private static boolean isNullOrEmpty(List<?> l) {
        return Objects.isNull(l) || l.isEmpty();
    }

    @Override
    public String queryStat(Select select) {
        throw new UnsupportedOperationException("ES");
    }

    @Override
    public void batchInsert(String table, List<Map<String, Object>> data) {
        throw new UnsupportedOperationException("current version not support ES");
    }

    @Override
    String buildURL(String host, int port, String dbName) {
        return host + ':' + port + '/' + dbName;
    }

    @Data
    @Accessors(chain = true)
    static class EsField {

        private String name;
        private String type;
        private String format;

        /**
         * for the text type
         */
        private List<EsField> fields;

        String getCondName() {
            if (!isText()) {
                return name;
            }

            if (Objects.isNull(fields)) {
                return name;
            }

            for (EsField f : fields) {

                if (!f.isText()) {
                    return name + "." + f.getName();
                }
            }

            throw new UnsupportedOperationException("no cond field:[" + name + "]");
        }

        /**
         * the field with type 'text' is used for the full text searching, cannot do the aggregation.
         * <p>
         * so return the sub-field if any
         *
         * @return
         */
        String getAggFieldName() {

            if (!isText()) {
                return name;
            }

            if (Objects.isNull(fields)) {
                throw new UnsupportedOperationException("cannot do aggregate on the text field:[" + name + "]");
            }

            for (EsField f : fields) {

                if (!f.isText()) {
                    return name + "." + f.getName();
                }
            }

            throw new UnsupportedOperationException("no aggregation field");
        }

        private boolean isText() {
            return TEXT_TYPE_NAME.equals(type);
        }

        static List<EsField> of(String name, Map<String, Object> m) {

            final String fieldsName = "fields";

            if (isText(m) && Objects.nonNull(m.get(fieldsName))) {

                Map<String, Map<String, Object>> fields = (Map<String, Map<String, Object>>) m.get(fieldsName);
                return ImmutableList.of(new EsField()
                    .setName(name)
                    .setFormat(format(m))
                    .setType(type(m))
                    .setFields(fields.entrySet().stream()
                        .map(e -> EsField.of(e.getKey(), e.getValue())).flatMap(Collection::stream)
                        .collect(Collectors.toList())));
            } else if (isProperties(m)) {
                Map<String, Map<String, Object>> props = (Map<String, Map<String, Object>>) m.get(PROPERTIES);
                return props.entrySet().stream()
                    .map(e -> EsField.of(name + "." + e.getKey(), e.getValue()))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            } else if (isGeoPoint(m)) {
                return ImmutableList.of(
                    new EsField()
                        .setName(name)
                        .setFormat(format(m))
                        .setType(type(m)),
                    new EsField()
                        .setName(name + LNG_SUFFIX)
                        .setFormat(format(m))
                        .setType("double"),
                    new EsField()
                        .setName(name + LAT_SUFFIX)
                        .setFormat(format(m))
                        .setType("double")
                );
            } else {
                return ImmutableList.of(new EsField()
                    .setName(name)
                    .setFormat(format(m))
                    .setType(type(m)));
            }
        }

        private static boolean isProperties(Map<String, Object> m) {
            return Objects.nonNull(m) && m.size() == 1 && Objects.nonNull(m.get(PROPERTIES));
        }

        private static boolean isText(Map<String, Object> m) {
            return Objects.nonNull(m) && TEXT_TYPE_NAME.equals(m.get("type"));
        }

        private static boolean isGeoPoint(Map<String, Object> m) {
            return Objects.nonNull(m) && GEO_TYPE_NAME.equals(m.get("type"));
        }

        private static String format(Map<String, Object> m) {
            return field(m, "format");
        }

        private static String type(Map<String, Object> m) {
            return field(m, "type");
        }

        private static String field(Map<String, Object> m, String name) {

            if (Objects.isNull(m)) {
                return "";
            }

            return (String) m.get(name);
        }
    }

    private void close(RestHighLevelClient client) {
        close(client, this.info);
    }

    private void close(RestHighLevelClient client, DB.ConnectInfo info) {

        if (Objects.isNull(client)) {
            return;
        }

        try {
            client.close();
        } catch (IOException e) {
            log.error("close client failed", e);
        } finally {
            CLIENT_CACHE.refresh(info);
        }
    }

    private RestHighLevelClient getAndCacheClient() {
        return getAndCacheClient(this.info);
    }

    private RestHighLevelClient getAndCacheClient(DB.ConnectInfo info) {
        return CLIENT_CACHE.get(info);
    }

    @Override
    protected String buildKey(DB.ConnectInfo info) {
        return info.getHost() + ":" + info.getPort();
    }
}
