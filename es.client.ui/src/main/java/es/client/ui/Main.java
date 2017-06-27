package es.client.ui;

import org.elasticsearch.client.Client;

public class Main {

    public static void main(String[] args) {
        //
        // TransportClientApp transportClientApp = new TransportClientApp();
        // Client client = transportClientApp.initTransportClient();

        AggregationExecutor aggExecutor = new AggregationExecutor();

        // aggExecutor.doBasicQuery(AggregationExecutor.BOUNCE, "now-6h",
        // "512041970");

        // Long sessionNonBounce = aggExecutor.getSessionNoBounce("sessionId",
        // "session_no_bound", QueryType.CARDINILITY, "now-6h",
        // "512041970", "", "");
        // aggExecutor.doQueryGroupbyOsDevice("now-3h", "512041970");
        //aggExecutor.queryFromAndTo("15/06/2017", "19/06/2017");

    }
}
