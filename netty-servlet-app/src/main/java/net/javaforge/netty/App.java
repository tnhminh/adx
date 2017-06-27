package net.javaforge.netty;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import net.javaforge.netty.jersey.MyApplication;
import net.javaforge.netty.servlet.bridge.ServletBridgeChannelPipelineFactory;
import net.javaforge.netty.servlet.bridge.config.ServletConfiguration;
import net.javaforge.netty.servlet.bridge.config.WebappConfiguration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class App {

	private static final Logger log = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) {

		long start = System.currentTimeMillis();

		// Configure the server.
		final ServerBootstrap bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		WebappConfiguration webapp = new WebappConfiguration()
				.addServletConfigurations(new ServletConfiguration(
						ServletContainer.class, "/*").addInitParameter(
						"javax.ws.rs.Application",
						MyApplication.class.getName()));

		// Set up the event pipeline factory.
		final ServletBridgeChannelPipelineFactory servletBridge = new ServletBridgeChannelPipelineFactory(
				webapp);
		bootstrap.setPipelineFactory(servletBridge);

		// Bind and start to accept incoming connections.
		final Channel serverChannel = bootstrap
				.bind(new InetSocketAddress(8080));

		long end = System.currentTimeMillis();

		log.info(">>> Server started in {} ms .... <<< ", (end - start));

		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				servletBridge.shutdown();
				serverChannel.close().awaitUninterruptibly();
				bootstrap.releaseExternalResources();
			}
		});

	}
}
