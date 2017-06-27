package es.client.ui;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class TransportClientApp {

    private static final String SANDBOX_CLUSTER_NAME = "sand-es-insight";

    private static final Logger LOGGER = Logger.getLogger("App-Client");

    public Client initTransportClient() {

        // Settings settings = Settings.builder().put("cluster.name",
        // SANDBOX_NODE_NAME).build();

        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", SANDBOX_CLUSTER_NAME)
                .put("client.transport.nodes_sampler_interval", 20, TimeUnit.SECONDS)
                // .put("client.transport.ping_timeout", 20, TimeUnit.SECONDS)
                .build();
        TransportClient client = new TransportClient(settings);

        client.addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9101)).addTransportAddress(
                new InetSocketTransportAddress("127.0.0.1", 9102));
        return client;
    }

    private void closeClient(Client client) {
        // on shutdown
        if (client != null) {
            client.close();
        }
    }
}
