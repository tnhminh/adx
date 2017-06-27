package es.client.ui;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.JSONObject;

public class JSONBuilder {

    private static final String TOTAL_RECORD = "total_record";
    private static final String DATA = "data";
    private static final String AUDIENCES = "audiences";
    private static final String METRICS = "metrics";
    private static final String AUDIENCE_ID = "audience_id";

    private static JSONObject createMetrics(Map<String, Object> metrics) {
        JSONObject metricsJSONObj = new JSONObject();
        for (Entry<String, Object> entry : metrics.entrySet()) {
            metricsJSONObj.put(entry.getKey(), entry.getValue());
        }
        return metricsJSONObj;
    }

    public static String createJSONObject(String field, Map<Map<String, Object>, Map<String, Object>> results) {

        JSONObject dataset = new JSONObject();
        JSONObject dataContent = new JSONObject();
        dataContent.put(TOTAL_RECORD, results.size());

        List<JSONObject> audiences = new ArrayList<JSONObject>();
        int i = 0;
        for (Entry<Map<String, Object>, Map<String, Object>> entry : results.entrySet()) {
            JSONObject audience1 = new JSONObject();
            Object object = entry.getKey().get(field);
            audience1.put(AUDIENCE_ID, "au-" + i);
            audience1.put(field, object);

            JSONObject metrics = createMetrics(entry.getValue());
            audience1.put(METRICS, metrics);

            audiences.add(audience1);
            i++;
        }

        dataContent.put(AUDIENCES, audiences);
        dataset.put(DATA, dataContent);
        return dataset.toString();
    }

    public static String createJSONObjectForBasicCounter(String field, Object result) {
        JSONObject dataset = new JSONObject();
        JSONObject dataContent = new JSONObject();
        dataContent.put(TOTAL_RECORD, 1);
        JSONObject audience1 = new JSONObject();
        audience1.put(AUDIENCE_ID, result.hashCode());
        audience1.put(field, result);
        dataContent.put("audience", audience1);
        dataset.put(DATA, dataContent);
        return dataset.toString();
    }

    public static String createJSONObjectForBasicCounter(Map<String, Object> results) {
        JSONObject dataset = new JSONObject();
        List<JSONObject> audiences = new ArrayList<JSONObject>();
        JSONObject dataContent = new JSONObject();
        dataContent.put(TOTAL_RECORD, 1);
        JSONObject audience = new JSONObject();
        audience.put(AUDIENCE_ID, results.hashCode());
        for (Entry<String, Object> entry : results.entrySet()) {
            audience.put(entry.getKey(), entry.getValue());
        }
        audiences.add(audience);
        dataContent.put(AUDIENCES, audiences);
        dataset.put(DATA, dataContent);

        return dataset.toString();
    }
}
