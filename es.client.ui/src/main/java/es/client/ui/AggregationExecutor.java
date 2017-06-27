package es.client.ui;

import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

public class AggregationExecutor {

    private static final int DEFAULT_INDEX = 0;

    private static final String COMMA = ",";

    private static final String URL_ID = "urlId";

    private static final int TIME_FOR_FULLY_DAY = 86400000;

    private static final int SECOND_IN_MINUTES = 60;

    private static final int SECOND_IN_HOUR = 3600;

    private static final String SITE_ID_FIELD = "siteId";

    private static final String INCR_BOUNCE_FIELD = "incrBounce";

    private static final String COUNT_BOUNCE = "count_bounce";

    private static final String OS_FAMILY_ID = "osFamilyId";

    protected static final String COUNT_USER_ID = "count_user_id";

    protected static final String NEW_USERS_AGG_NAME = "new_users_agg";

    protected static final String COUNT_TIME_SPENT = "count_time_spent";

    protected static final String COUNT_DISTINT_SESSION_ID = "count_distint_sessionId";

    protected static final String SESSION_ID = "sessionId";

    protected static final String LOGGED_DATE = "loggedDate";

    protected static final String AGG_NAME = "agg_name";

    protected static final String DISTINT_INDEX = "distint_index";

    protected static final String _INDEX = "_index";

    protected static final String USERS_PHRASE = "users";

    protected static final int USERS = 4;

    protected static final String TOTAL_NEW_USER_PHRASE = "total_new_users";

    protected static final int TOTAL_NEW_USER = 5;

    protected static final String BOUNCE_PHRASE = "total_bounce";

    protected static final int BOUNCE = 6;

    protected static final String SESSION_NO_BOUNCE = "session_no_bounce";

    protected static final String VISITOR_ID = "visitorId";

    protected static final String NEW_VISITOR_ID = "newVisit";

    protected static final String COUNT = "count";

    protected static final String SUM_PAGEVIEWS = "sum_pageviews";

    protected static final String QUERY_PAGE_AND_USERS = "query_page_and_users";

    protected static final String LOGGED_TIME = "loggedTime";

    protected static final String GROUP_BY_OS_FAMILY_ID = "group_by_osFamilyId";

    protected static final int PAGE_VIEW = 1;

    protected static final int SESSION = 2;

    protected static final int TIME_SPENT = 3;

    protected static final String TIME_SPENT_FIELD = "timeSpent";

    protected static final int BOUNCE_RATE = 10;

    protected static final int PERCENT_NEW_SESSION = 13;

    private String[] m_indices;

    protected static ImmutableOpenMap<String, IndexMetaData> m_indicesCacheMap;

    private Client m_client;

    public AggregationExecutor() {

        TransportClientApp transportClientApp = new TransportClientApp();
        m_client = transportClientApp.initTransportClient();
        m_indices = initializeIndices();
    }

    public void queryPageAndUsers() {

        AggregationBuilder aggregation = AggregationBuilders.terms(QUERY_PAGE_AND_USERS).field(LOGGED_TIME)
                .subAggregation(AggregationBuilders.sum(SUM_PAGEVIEWS).field(COUNT))
                .subAggregation(AggregationBuilders.cardinality(USERS_PHRASE).field(VISITOR_ID));

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices).setQuery(QueryBuilders.matchAllQuery()).setSearchType(SearchType.COUNT)
                .setSize(10).addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        Terms aggTerms = response.getAggregations().get(QUERY_PAGE_AND_USERS);

        for (Bucket bucket : aggTerms.getBuckets()) {

            System.out.println("Date:" + bucket.getKey());

            Sum sumPageviews = (Sum) bucket.getAggregations().get(SUM_PAGEVIEWS);
            Cardinality cardinatyUsers = (Cardinality) bucket.getAggregations().get(USERS_PHRASE);

            System.out.println("Pageviews:" + ((Double) sumPageviews.getValue()).longValue());
            System.out.println("Users:" + cardinatyUsers.getValue());
        }
    }

    public String[] filterRangeDateIndices(String fromDate, String toDate, int lastTimes, String timeType) {
        Long fromMil;
        Long toMil;
        if (!isValidDate(fromDate, toDate)) {
            long resultsFr = 0;
            long timeMiliSeconds = System.currentTimeMillis();
            if ("h".equals(timeType)) {
                resultsFr = timeMiliSeconds - (lastTimes * SECOND_IN_HOUR * 1000);
            } else if ("m".equals(timeType)) {
                resultsFr = timeMiliSeconds - (lastTimes * SECOND_IN_MINUTES * 1000);
            }
            fromMil = resultsFr;
            toMil = null;
        } else {
            fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            toMil = ParseUtil.parseDateToMiliseconds(toDate);
        }

        FilterBuilder filter;
        String finalLoggedType;
        if (lastTimes > 0) {
            finalLoggedType = LOGGED_TIME;
        } else {
            finalLoggedType = LOGGED_DATE;
        }

        filter = rangeFilter(finalLoggedType).gte(fromMil).lte(toMil).includeLower(true).includeUpper(true);

        AggregationBuilder aggregation = AggregationBuilders.terms(DISTINT_INDEX).field(_INDEX).order(Order.term(true));

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filter)).setSize(0).addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        Terms aggTerms = response.getAggregations().get(DISTINT_INDEX);

        List<String> fromToDate = new ArrayList<String>();

        for (Bucket bucket : aggTerms.getBuckets()) {
            if (bucket.getKey().contains("analytics")) {
                fromToDate.add(bucket.getKey());
                System.out.println("Date:" + bucket.getKey());
            }
        }

        String[] result = new String[0];
        if (!fromToDate.isEmpty()) {
            result = (String[]) fromToDate.toArray(new String[fromToDate.size()]);
        }

        return result;
    }

    public Map<Map<String, Object>, Map<String, Object>> doQueryGroupbyOsDevice(String dateType, String siteId, String fromDate, String toDate,
            boolean isSorted, String sortType, String sortField) {

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));

        if (isValidDateType(dateType)) {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        } else {
            Long fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            Long toMil = ParseUtil.parseDateToMiliseconds(toDate) + 86400000;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        }

        AggregationBuilder aggregation = null;
        if (isSorted) {
            boolean isAsc = false;
            if (sortType.equals("asc")) {
                isAsc = true;
            }
            aggregation = AggregationBuilders.terms(GROUP_BY_OS_FAMILY_ID).field(OS_FAMILY_ID)
                    .subAggregation(AggregationBuilders.sum("sum_pageview").field("count")).order(Terms.Order.aggregation(sortField, isAsc));
        } else {
            aggregation = AggregationBuilders.terms(GROUP_BY_OS_FAMILY_ID).field(OS_FAMILY_ID)
                    .subAggregation(AggregationBuilders.sum("sum_pageview").field("count"));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT).setSize(0)
                .addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        Terms aggTerms = response.getAggregations().get(GROUP_BY_OS_FAMILY_ID);
        Map<Map<String, Object>, Map<String, Object>> resultOss = new LinkedHashMap<Map<String, Object>, Map<String, Object>>();

        // For each entry
        for (Terms.Bucket entry : aggTerms.getBuckets()) {
            Map<String, Object> metrics = new LinkedHashMap<String, Object>();
            Map<String, Object> keys = new LinkedHashMap<String, Object>();
            String key = entry.getKey(); // bucket key
            keys.put(OS_FAMILY_ID, key);
            Sum sum = entry.getAggregations().get("sum_pageview");
            metrics.put(sum.getName(), Double.valueOf(sum.getValue()).longValue());
            resultOss.put(keys, metrics);
        }
        return resultOss;
    }

    public Map<Map<String, Object>, Map<String, Object>> doMultiRequestForDays(String siteId, String[] indices, boolean isLoggedTime, int lastTimes,
            String timeType) {
        LinkedHashMap<Map<String, Object>, Map<String, Object>> multiRequestResults = new LinkedHashMap<Map<String, Object>, Map<String, Object>>();
        String fieldFinal = isLoggedTime ? LOGGED_TIME : LOGGED_DATE;
        AggregationBuilder aggregation = AggregationBuilders.terms(AGG_NAME).field(fieldFinal)
                .subAggregation(AggregationBuilders.sum(SUM_PAGEVIEWS).field(COUNT))
                .subAggregation(AggregationBuilders.cardinality(USERS_PHRASE).field(VISITOR_ID));

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));
        String type = "";
        String dateType = "";
        if (lastTimes > 0) {
            type = timeType.equals("h") ? "h" : "m";
            dateType = "now" + "-" + lastTimes + type;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        }

        MultiSearchRequestBuilder multiSearchRequest = m_client.prepareMultiSearch();
        if (indices == null || Arrays.asList(indices).isEmpty()) {
            return multiRequestResults;
        }
        for (String index : Arrays.asList(indices)) {
            SearchRequestBuilder srb1 = m_client.prepareSearch(index).setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder))
                    .setSearchType(SearchType.COUNT).setSize(0).addAggregation(aggregation);
            multiSearchRequest.add(srb1);
        }

        MultiSearchResponse response = multiSearchRequest.execute().actionGet();

        for (MultiSearchResponse.Item item : response.getResponses()) {
            try {
                LongTerms longTerm = item.getResponse().getAggregations().get(AGG_NAME);
                for (Bucket bucket : longTerm.getBuckets()) {
                    Map<String, Object> values = new HashMap<String, Object>();
                    Map<String, Object> keys = new LinkedHashMap<String, Object>();
                    Calendar calendar = Calendar.getInstance();
                    // Set local time (not UTC)
                    calendar.setTimeZone(TimeZone.getDefault());
                    calendar.setTimeInMillis(bucket.getKeyAsNumber().longValue());

                    Sum sumPageviews = (Sum) bucket.getAggregations().get(SUM_PAGEVIEWS);
                    Cardinality cardinatyUsers = (Cardinality) bucket.getAggregations().get(USERS_PHRASE);
                    values.put(sumPageviews.getName(), sumPageviews.getValue());
                    values.put(cardinatyUsers.getName(), cardinatyUsers.getValue());
                    keys.put("dateTime", calendar.getTime().toString());
                    multiRequestResults.put(keys, values);
                }
            } catch (Exception e) {
                System.out.println("Error when process query result !");
                e.printStackTrace();
            }
        }
        return multiRequestResults;
    }

    protected String[] initializeIndices() {

        if (m_client != null) {
            m_indicesCacheMap = getIndicesMapCache();
        }

        String[] myIndexes = (String[]) m_indicesCacheMap.keys().toArray(String.class);
        List<String> filteredIndexes = new ArrayList<String>();
        for (String s : Arrays.asList(myIndexes)) {
            if (s.contains("analytics")) {
                filteredIndexes.add(s);
            }
        }
        String[] array = filteredIndexes.toArray(new String[filteredIndexes.size()]);
        return array;
    }

    /**
     * Get all indices
     * 
     * @param client
     * @return ImmutableOpenMap<String, IndexMetaData>
     */
    protected ImmutableOpenMap<String, IndexMetaData> getIndicesMapCache() {
        ImmutableOpenMap<String, IndexMetaData> indices = m_client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices();
        return indices;
    }

    /**
     * Query with from to option
     * 
     * @param client
     * @param from
     * @param to
     */
    public Map<Map<String, Object>, Map<String, Object>> queryFromAndTo(String siteId, int lastHours, String from, String to, String timeType) {
        String[] fromToIndices = filterRangeDateIndices(from, to, lastHours, timeType);
        boolean isLoggedTime = false;
        if (lastHours > 0 || timeType.equals("h") || timeType.equals("m")) {
            isLoggedTime = true;
        }
        return doMultiRequestForDays(siteId, fromToIndices, isLoggedTime, lastHours, timeType);
    }

    public Map<Map<String, Object>, Map<String, Object>> queryHistogramDate(String siteId, DateHistogram.Interval interval, String fromDate, String toDate,
            String dateType) {

        String finalField = "";
        if (isLoggedDate(interval)) {
            finalField = LOGGED_DATE;
        } else {
            finalField = LOGGED_TIME;
        }

        AggregationBuilder aggregation = AggregationBuilders.dateHistogram(AGG_NAME).field(finalField).interval(interval)
                .subAggregation(AggregationBuilders.sum(SUM_PAGEVIEWS).field(COUNT))
                // Please note that users may be duplicated and give misalign
                // value when change interval from DAY to MONTH
                .subAggregation(AggregationBuilders.cardinality(USERS_PHRASE).field(VISITOR_ID));

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));

        Long fromMil = 0L;
        Long toMil = 0L;
        if (isValidDate(fromDate, toDate)) {
            fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
        }

        if (isValidDateType(dateType)) {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        } else {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT).setSize(10)
                .addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        DateHistogram dateHistogram = response.getAggregations().get(AGG_NAME);
        Map<Map<String, Object>, Map<String, Object>> results = new HashMap<Map<String, Object>, Map<String, Object>>();

        for (org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Bucket bucket : dateHistogram.getBuckets()) {
            Map<String, Object> keys = new LinkedHashMap<String, Object>();
            Map<String, Object> valueForDates = new HashMap<String, Object>();
            keys.put("dateTime", bucket.getKey());
            Cardinality cardinatyUsers = (Cardinality) bucket.getAggregations().get(USERS_PHRASE);
            Sum sumPageView = ((Sum) bucket.getAggregations().get(SUM_PAGEVIEWS));
            valueForDates.put(cardinatyUsers.getName(), cardinatyUsers.getValue());
            valueForDates.put(sumPageView.getName(), sumPageView.getValue());

            results.put(keys, valueForDates);
        }
        return results;
    }

    private boolean isLoggedDate(DateHistogram.Interval interval) {
        return interval.toString().equals(DateHistogram.Interval.DAY.toString()) || interval.toString().equals(DateHistogram.Interval.WEEK.toString())
                || interval.toString().equals(DateHistogram.Interval.YEAR.toString()) || interval.toString().equals(DateHistogram.Interval.MONTH.toString());
    }

    public double getPercentNewSession(String fieldName, String nameAgg, String dateType, String siteId, String fromDate, String toDate) {

        FilterAggregationBuilder aggregationSessionNonBounce = AggregationBuilders.filter(nameAgg + "_filters")
                .filter(FilterBuilders.termFilter("newVisit", "1")).subAggregation(AggregationBuilders.cardinality(nameAgg).field(fieldName));

        // Freedom aggregation
        AbstractAggregationBuilder sessionAgg = (AbstractAggregationBuilder) AggregationBuilders.cardinality("sessions_agg").field("sessionId");

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));

        Long fromMil;
        Long toMil;
        if (isValidDate(fromDate, toDate)) {
            fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        } else {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices).setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT)
                .addAggregation(aggregationSessionNonBounce).addAggregation(sessionAgg);

        SearchResponse response = searchRequestBuilder.get();

        InternalFilter fillterAggResponse = (InternalFilter) response.getAggregations().get(nameAgg + "_filters");

        long sessionNonBounces = ((Cardinality) fillterAggResponse.getAggregations().get(nameAgg)).getValue();
        System.out.println("New Session: " + sessionNonBounces);
        long sessions = ((Cardinality) response.getAggregations().get("sessions_agg")).getValue();
        System.out.println("Total Session: " + sessions);

        return 100 * ((double) sessionNonBounces / sessions);
    }

    public double getBounceRate(String fieldName, String dateType, String siteId, String fromDate, String toDate) {

        String aggName = "bounce_rate";
        FilterAggregationBuilder aggregationSessionNonBounce = AggregationBuilders.filter(aggName + "_filters")
                .filter(FilterBuilders.termFilter("incrBounce", "0")).subAggregation(AggregationBuilders.cardinality(aggName).field(fieldName));

        AbstractAggregationBuilder sessionAgg = (AbstractAggregationBuilder) AggregationBuilders.cardinality("sessions_agg").field("sessionId");

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));

        Long fromMil;
        Long toMil;
        if (isValidDate(fromDate, toDate)) {
            fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        } else {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices).setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT)
                .addAggregation(aggregationSessionNonBounce).addAggregation(sessionAgg);

        SearchResponse response = searchRequestBuilder.get();

        InternalFilter fillterAggResponse = (InternalFilter) response.getAggregations().get(aggName + "_filters");

        long sessionNonBounces = ((Cardinality) fillterAggResponse.getAggregations().get(aggName)).getValue();
        System.out.println("Session Non Bounce: " + sessionNonBounces);
        long sessions = ((Cardinality) response.getAggregations().get("sessions_agg")).getValue();
        System.out.println("Total Sessions: " + sessions);

        return 100 * ((double) (sessions - sessionNonBounces) / sessions);
    }

    public Long getBasicCounterValue(String fieldName, String nameAgg, QueryType typeAgg, String dateType, String siteId, String fromDate, String toDate) {

        boolean isCardinility = true;
        MetricsAggregationBuilder aggregation;

        if (QueryType.CARDINILITY.equals(typeAgg)) {
            aggregation = AggregationBuilders.cardinality(nameAgg).field(fieldName);
        } else {
            aggregation = AggregationBuilders.sum(nameAgg).field(fieldName);
            isCardinility = false;
        }

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter(SITE_ID_FIELD, siteId));

        if (isValidDate(fromDate, toDate)) {
            Long fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            Long toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        } else {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT).setSize(10)
                .addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        if (isCardinility) {
            return ((Cardinality) response.getAggregations().get(nameAgg)).getValue();
        } else {
            return (long) ((Sum) response.getAggregations().get(nameAgg)).getValue();
        }
    }

    private boolean isValidDate(String fromDate, String toDate) {
        return fromDate != null && toDate != null && !fromDate.isEmpty() && !toDate.isEmpty();
    }

    public Long getBasicCounterValue(String fieldName, String nameAgg, QueryType typeAgg, String dateType, String siteId) {
        return getBasicCounterValue(fieldName, nameAgg, typeAgg, dateType, siteId, null, null);
    }

    public Map<Map<String, Object>, Map<String, Object>> getArticles(String siteId, String dateType, String fromDate, String toDate, boolean isSorted,
            String sortType, String sortField) {

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter("siteId", siteId));

        if (isValidDateType(dateType)) {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        } else {
            Long fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            Long toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        }
        AggregationBuilder aggregation = AggregationBuilders.terms("group_by_url_id").field(URL_ID);

        TermsBuilder sumPageAgg = (TermsBuilder) aggregation.subAggregation(AggregationBuilders.sum(SUM_PAGEVIEWS).field(COUNT));

        TermsBuilder usersAgg = (TermsBuilder) aggregation.subAggregation(AggregationBuilders.cardinality(USERS_PHRASE).field(VISITOR_ID));

        if (isSorted) {
            boolean isAsc = false;
            if (sortType.equals("asc")) {
                isAsc = true;
            }
            if (sortField.equals(SUM_PAGEVIEWS)) {
                sumPageAgg.order(Terms.Order.aggregation(sortField, isAsc));
            } else {
                usersAgg.order(Terms.Order.aggregation(sortField, isAsc));
            }
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT).setSize(0)
                .addAggregation(aggregation);

        SearchResponse response = searchRequestBuilder.get();

        Terms aggTerms = response.getAggregations().get("group_by_url_id");
        Map<Map<String, Object>, Map<String, Object>> results = new LinkedHashMap<Map<String, Object>, Map<String, Object>>();
        // For each entry
        for (Terms.Bucket entry : aggTerms.getBuckets()) {
            Map<String, Object> keys = new LinkedHashMap<String, Object>();
            Map<String, Object> metrics = new LinkedHashMap<String, Object>();

            Cardinality cardinatyUsers = (Cardinality) entry.getAggregations().get(USERS_PHRASE);
            Sum sumPageView = ((Sum) entry.getAggregations().get(SUM_PAGEVIEWS));
            metrics.put(cardinatyUsers.getName(), cardinatyUsers.getValue());
            metrics.put(sumPageView.getName(), sumPageView.getValue());

            String key = entry.getKey(); // bucket key
            keys.put(URL_ID, key);
            results.put(keys, metrics);
        }
        return results;
    }

    private boolean isValidDateType(String dateType) {
        return dateType != null && !dateType.isEmpty();
    }

    public Long doBasicExtendQuery(int queryType, String dateType, String siteId, String fromDate, String toDate) {
        switch (queryType) {
        case PAGE_VIEW:
            return getBasicCounterValue(COUNT, SUM_PAGEVIEWS, QueryType.SUM, dateType, siteId, fromDate, toDate);
        case SESSION:
            return getBasicCounterValue(SESSION_ID, COUNT_DISTINT_SESSION_ID, QueryType.CARDINILITY, dateType, siteId, fromDate, toDate);
        case TIME_SPENT:
            return getBasicCounterValue(TIME_SPENT_FIELD, COUNT_TIME_SPENT, QueryType.CARDINILITY, dateType, siteId, fromDate, toDate);
        case USERS:
            return getBasicCounterValue(VISITOR_ID, COUNT_USER_ID, QueryType.CARDINILITY, dateType, siteId, fromDate, toDate);
        case TOTAL_NEW_USER:
            return getBasicCounterValue(NEW_VISITOR_ID, NEW_USERS_AGG_NAME, QueryType.CARDINILITY, dateType, siteId, fromDate, toDate);
        case BOUNCE:
            return getBasicCounterValue(INCR_BOUNCE_FIELD, COUNT_BOUNCE, QueryType.SUM, dateType, siteId, fromDate, toDate);
        default:
            break;
        }
        return null;
    }

    public Map<String, Object> doBasicQueryWithComplexType(int queryType, String dateType, String siteId, String fromDate, String toDate,
            List<String> complexTypes) {
        return getBasicComplexCounterValue(siteId, dateType, fromDate, toDate, complexTypes);
    }

    public Map<String, Object> getBasicComplexCounterValue(String siteId, String dateType, String fromDate, String toDate, List<String> complexTypes) {

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter().must(FilterBuilders.termFilter(SITE_ID_FIELD, siteId));
        List<String> percentQueryTypes = new ArrayList<String>();

        if (isValidDate(fromDate, toDate)) {
            Long fromMil = ParseUtil.parseDateToMiliseconds(fromDate);
            Long toMil = ParseUtil.parseDateToMiliseconds(toDate) + TIME_FOR_FULLY_DAY;
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(fromMil).lte(toMil));
        } else {
            boolFilterBuilder.must(FilterBuilders.rangeFilter(LOGGED_TIME).gte(dateType));
        }

        SearchRequestBuilder searchRequestBuilder = m_client.prepareSearch(m_indices)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), boolFilterBuilder)).setSearchType(SearchType.COUNT).setSize(10);

        String[] splitQueryTypes = complexTypes.get(DEFAULT_INDEX).split(COMMA);
        for (String queryType : splitQueryTypes) {

            boolean isFilterQuery = filterPercentQueryType(percentQueryTypes, queryType);
            if (isFilterQuery) {
                continue;
            }

            boolean isDistint = false;
            QueryType queryTypeBasic = ParseUtil.MAPPING_QUERY_TYPE_WITH_CAL.get(queryType);
            if (queryTypeBasic.equals(QueryType.CARDINILITY)) {
                isDistint = true;
            }

            addAggregation(searchRequestBuilder, queryType, isDistint);
        }

        Map<String, Object> results = new HashMap<String, Object>();
        SearchResponse response = searchRequestBuilder.get();
        for (String queryType : splitQueryTypes) {
            QueryType queryBasic = ParseUtil.MAPPING_QUERY_TYPE_WITH_CAL.get(queryType);
            if (queryBasic == null) {
                continue;
            }
            if (queryBasic.equals(QueryType.CARDINILITY)) {
                results.put(queryType, ((Cardinality) response.getAggregations().get(queryType)).getValue());
            } else {
                results.put(queryType, ((Sum) response.getAggregations().get(queryType)).getValue());
            }
        }

        updatePercentQueryResults(siteId, dateType, fromDate, toDate, percentQueryTypes, results);
        return results;
    }

    private void updatePercentQueryResults(String siteId, String dateType, String fromDate, String toDate, List<String> percentQueryTypes,
            Map<String, Object> results) {
        for (String queryType : percentQueryTypes) {
            if (queryType.equals(ParseUtil.BOUNCE_RATE)) {
                results.put(queryType, getBounceRate(SESSION_ID, dateType, siteId, fromDate, toDate));
            } else {
                Object sessionTotal = results.get(ParseUtil.SESSION_TOTAL);
                if (queryType.equals(ParseUtil.PAGE_ON_SESSION)) {
                    Object pageViewTotal = results.get(ParseUtil.PAGEVIEW_TOTAL);

                    if (pageViewTotal == null) {
                        pageViewTotal = doBasicExtendQuery(PAGE_VIEW, dateType, siteId, fromDate, toDate);
                    }

                    if (sessionTotal == null) {
                        sessionTotal = doBasicExtendQuery(SESSION, dateType, siteId, fromDate, toDate);
                    }
                    double pageOnSession = ((Double) pageViewTotal).doubleValue() / ((Long) sessionTotal).longValue();
                    results.put(ParseUtil.PAGE_ON_SESSION, pageOnSession);
                } else if (queryType.equals(ParseUtil.AVG_SESSION_DURATION)) {
                    Object timeSpentTotal = results.get(ParseUtil.TIME_SPENT_TOTAL);
                    if (timeSpentTotal == null) {
                        timeSpentTotal = doBasicExtendQuery(TIME_SPENT, dateType, siteId, fromDate, toDate);
                    }
                    Double avgSessionDuration = (double) (((Long) timeSpentTotal).doubleValue() / ((Long) sessionTotal));
                    results.put(queryType, avgSessionDuration);
                } else if (queryType.equals(ParseUtil.PERCENT_NEW_SESSION)) {
                    results.put(queryType, getPercentNewSession(SESSION_ID, ParseUtil.PERCENT_NEW_SESSION, dateType, siteId, fromDate, toDate));
                }
            }
        }
    }

    private void addAggregation(SearchRequestBuilder searchRequestBuilder, String queryType, boolean isDistint) {
        if (isDistint) {
            searchRequestBuilder.addAggregation(AggregationBuilders.cardinality(queryType).field(ParseUtil.MAPPING_QUERY_TYPE_WITH_FIELD.get(queryType)));
        } else {
            searchRequestBuilder.addAggregation(AggregationBuilders.sum(queryType).field(ParseUtil.MAPPING_QUERY_TYPE_WITH_FIELD.get(queryType)));
        }
    }

    private boolean filterPercentQueryType(List<String> percentQueryType, String queryType) {
        if (ParseUtil.QUERY_TYPE_PERCENT.containsKey(queryType)) {
            percentQueryType.add(queryType);
            return true;
        }
        return false;
    }

    public Object doPercentCounter(int queryType, String dateType, String siteId, String fromDate, String toDate) {
        switch (queryType) {
        case BOUNCE_RATE:
            return getBounceRate("sessionId", dateType, siteId, fromDate, toDate);
        case PERCENT_NEW_SESSION:
            return getPercentNewSession("sessionId", "percent_new_session", dateType, siteId, fromDate, toDate);
        default:
            break;
        }
        return null;
    }
}
