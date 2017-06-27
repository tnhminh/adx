package es.client.ui;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ParseUtil {

    public static final String PERCENT_NEW_SESSION = "percent_new_session";

    public static final String AVG_SESSION_DURATION = "avg_session_duration";

    public static final String PAGE_ON_SESSION = "page_on_session";

    public static final String BOUNCE_RATE = "bounce_rate";

    public static final String BOUNCE_TOTAL = "bounce_total";

    public static final String NEW_USER_TOTAL = "new_user_total";

    public static final String USER_TOTAL = "user_total";

    public static final String TIME_SPENT_TOTAL = "time_spent_total";

    public static final String SESSION_TOTAL = "session_total";

    public static final String PAGEVIEW_TOTAL = "pageview_total";

    protected static final String DD_MM_YYYY = "dd-MM-yyyy";

    public static final Map<String, String> MAPPING_QUERY_TYPE_WITH_FIELD = new HashMap<String, String>();
    static {
        MAPPING_QUERY_TYPE_WITH_FIELD.put(PAGEVIEW_TOTAL, "count");
        MAPPING_QUERY_TYPE_WITH_FIELD.put(SESSION_TOTAL, "sessionId");
        MAPPING_QUERY_TYPE_WITH_FIELD.put(TIME_SPENT_TOTAL, "timeSpent");
        MAPPING_QUERY_TYPE_WITH_FIELD.put(USER_TOTAL, "visitorId");
        MAPPING_QUERY_TYPE_WITH_FIELD.put(NEW_USER_TOTAL, "newVisit");
        MAPPING_QUERY_TYPE_WITH_FIELD.put(BOUNCE_TOTAL, "incrBounce");
    }

    public static final Map<String, List<String>> QUERY_TYPE_PERCENT = new HashMap<String, List<String>>();
    static {

        QUERY_TYPE_PERCENT.put(BOUNCE_RATE, Arrays.asList(new String[] { SESSION_TOTAL }));
        QUERY_TYPE_PERCENT.put(PAGE_ON_SESSION, Arrays.asList(new String[] { PAGEVIEW_TOTAL, SESSION_TOTAL }));
        QUERY_TYPE_PERCENT.put(AVG_SESSION_DURATION, Arrays.asList(new String[] { TIME_SPENT_TOTAL, SESSION_TOTAL }));
        QUERY_TYPE_PERCENT.put(PERCENT_NEW_SESSION, Arrays.asList(new String[] { SESSION_TOTAL }));

    }

    public static final Map<String, QueryType> MAPPING_QUERY_TYPE_WITH_CAL = new HashMap<String, QueryType>();
    static {
        MAPPING_QUERY_TYPE_WITH_CAL.put(PAGEVIEW_TOTAL, QueryType.SUM);
        MAPPING_QUERY_TYPE_WITH_CAL.put(SESSION_TOTAL, QueryType.CARDINILITY);
        MAPPING_QUERY_TYPE_WITH_CAL.put(TIME_SPENT_TOTAL, QueryType.CARDINILITY);
        MAPPING_QUERY_TYPE_WITH_CAL.put(USER_TOTAL, QueryType.CARDINILITY);
        MAPPING_QUERY_TYPE_WITH_CAL.put(NEW_USER_TOTAL, QueryType.CARDINILITY);
        MAPPING_QUERY_TYPE_WITH_CAL.put(BOUNCE_TOTAL, QueryType.SUM);
    }

    public static Long parseDateToMiliseconds(String dateString) {

        SimpleDateFormat sdf = new SimpleDateFormat(DD_MM_YYYY);
        sdf.setTimeZone(TimeZone.getDefault());

        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public static int parseQueryTypeToNumber(String queryType) {
        if (queryType.equals(PAGEVIEW_TOTAL)) {
            return 1;
        } else if (queryType.equals(SESSION_TOTAL)) {
            return 2;
        } else if (queryType.equals(TIME_SPENT_TOTAL)) {
            return 3;
        } else if (queryType.equals(USER_TOTAL)) {
            return 4;
        } else if (queryType.equals(NEW_USER_TOTAL)) {
            return 5;
        } else if (queryType.equals(BOUNCE_TOTAL)) {
            return 6;
        } else if (queryType.equals(BOUNCE_RATE)) {
            return 10;
        } else if (queryType.equals(PAGE_ON_SESSION)) {
            return 11;
        } else if (queryType.equals(AVG_SESSION_DURATION)) {
            return 12;
        } else if (queryType.equals(PERCENT_NEW_SESSION)) {
            return 13;
        }
        return 0;
    }
}
