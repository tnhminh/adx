package es.client.ui;

import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogram.Interval;

public class ComparableInterval extends Interval {

    public ComparableInterval(String expression) {
        super(expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this.DAY.equals(((Interval) obj).DAY) || this.WEEK.equals(((Interval) obj).WEEK) || this.YEAR.equals(((Interval) obj).YEAR)) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 1;
    }

}
