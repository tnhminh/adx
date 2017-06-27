package net.javaforge.netty.jersey;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;

import es.client.ui.AggregationExecutor;
import es.client.ui.JSONBuilder;
import es.client.ui.ParseUtil;

@Path("/query")
public class QueryResource {

    @GET
    @Produces("text/plain")
    @Path("/multiRequest")
    public String queryMultiRequest(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "fromDate") String fromDate,
            @QueryParam(value = "toDate") String toDate, @QueryParam(value = "timeType") String timeType, @QueryParam(value = "lastHour") int lastHour) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String createJSONObject = JSONBuilder.createJSONObject("dateTime", aggEx.queryFromAndTo(siteId, lastHour, fromDate, toDate, timeType));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObject, durationTime);
    }

    @GET
    @Produces("text/plain")
    @Path("/osdevice")
    public String queryGroupByOsDevice(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "dateType") String dateType,
            @QueryParam(value = "fromDate") String fromDate, @QueryParam(value = "toDate") String toDate, @QueryParam(value = "isSorted") boolean isSorted,
            @QueryParam(value = "sortType") String sortType, @QueryParam(value = "sortField") String sortField) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String createJSONObject = JSONBuilder.createJSONObject("osFamilyId",
                aggEx.doQueryGroupbyOsDevice(dateType, siteId, fromDate, toDate, isSorted, sortType, sortField));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObject, durationTime);
    }

    @GET
    @Produces("text/plain")
    @Path("/dateHistogram")
    public String queryHistogramDate(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "interval") Interval interval,
            @QueryParam(value = "fromDate") String fromDate, @QueryParam(value = "toDate") String toDate, @QueryParam(value = "dateType") String dateType) {
        AggregationExecutor aggEx = new AggregationExecutor();
        long currentTime = System.currentTimeMillis();
        String createJSONObject = JSONBuilder.createJSONObject("dateTime", aggEx.queryHistogramDate(siteId, interval, fromDate, toDate, dateType));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObject, durationTime);
    }

    private String appendDurationTimeToResult(String createJSONObject, long durationTime) {
        return "[" + createJSONObject + "," + "{\"took\"" + ":" + durationTime + "," + "\"type\"" + ":" + "\"(ms)\"" + "}]";
    }

    @GET
    @Produces("text/plain")
    @Path("/basicQuery")
    public String getBasicCounter(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "queryType") String queryType,
            @QueryParam(value = "fromDate") String fromDate, @QueryParam(value = "toDate") String toDate, @QueryParam(value = "dateType") String dateType) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String createJSONObjectForBasicCounter = JSONBuilder.createJSONObjectForBasicCounter(queryType,
                aggEx.doBasicExtendQuery(ParseUtil.parseQueryTypeToNumber(queryType), dateType, siteId, fromDate, toDate));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObjectForBasicCounter, durationTime);
    }

    @GET
    @Produces("text/plain")
    @Path("/basicQuery/_complex")
    public String getComplexBasicCounter(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "fromDate") String fromDate,
            @QueryParam(value = "toDate") String toDate, @QueryParam(value = "dateType") String dateType,
            @QueryParam(value = "queryTypes") List<String> queryTypes) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String createJSONObjectForBasicCounter = JSONBuilder.createJSONObjectForBasicCounter(aggEx.getBasicComplexCounterValue(siteId, dateType, fromDate,
                toDate, queryTypes));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObjectForBasicCounter, durationTime);
    }

    @GET
    @Produces("text/plain")
    @Path("/basicPercent")
    public String getPercentRateCounter(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "queryType") String queryType,
            @QueryParam(value = "dateType") String dateType, @QueryParam(value = "fromDate") String fromDate, @QueryParam(value = "toDate") String toDate) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String result = "";
        if (queryType.equals("page_on_session")) {
            Long pageViews = aggEx.doBasicExtendQuery(ParseUtil.parseQueryTypeToNumber("pageview_total"), dateType, siteId, fromDate, toDate);
            Long sessions = aggEx.doBasicExtendQuery(ParseUtil.parseQueryTypeToNumber("session_total"), dateType, siteId, fromDate, toDate);
            result = JSONBuilder.createJSONObjectForBasicCounter("page_on_session", String.valueOf((double) pageViews / sessions));
        } else if (queryType.equals("bounce_rate")) {
            result = JSONBuilder.createJSONObjectForBasicCounter("bounce_rate",
                    aggEx.doPercentCounter(ParseUtil.parseQueryTypeToNumber(queryType), dateType, siteId, fromDate, toDate));
        } else if (queryType.equals("avg_session_duration")) {
            Long timeSpent = aggEx.doBasicExtendQuery(ParseUtil.parseQueryTypeToNumber("time_spent_total"), dateType, siteId, fromDate, toDate);
            Long sessions = aggEx.doBasicExtendQuery(ParseUtil.parseQueryTypeToNumber("session_total"), dateType, siteId, fromDate, toDate);
            result = JSONBuilder.createJSONObjectForBasicCounter("avg_session_duration", String.valueOf((double) timeSpent / sessions));
        } else if (queryType.equals("percent_new_session")) {
            result = JSONBuilder.createJSONObjectForBasicCounter("percent_new_session",
                    aggEx.doPercentCounter(ParseUtil.parseQueryTypeToNumber(queryType), dateType, siteId, fromDate, toDate));
        }
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(result, durationTime);
    }

    @GET
    @Produces("text/plain")
    @Path("/articles")
    public String getArticles(@QueryParam(value = "siteId") String siteId, @QueryParam(value = "fromDate") String fromDate,
            @QueryParam(value = "toDate") String toDate, @QueryParam(value = "dateType") String dateType, @QueryParam(value = "isSorted") boolean isSorted,
            @QueryParam(value = "sortType") String sortType, @QueryParam(value = "sortField") String sortField) {
        long currentTime = System.currentTimeMillis();
        AggregationExecutor aggEx = new AggregationExecutor();
        String createJSONObject = JSONBuilder.createJSONObject("urlId", aggEx.getArticles(siteId, dateType, fromDate, toDate, isSorted, sortType, sortField));
        long durationTime = System.currentTimeMillis() - currentTime;
        return appendDurationTimeToResult(createJSONObject, durationTime);
    }

}