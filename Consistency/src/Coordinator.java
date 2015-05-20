import java.io.IOException;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	// Default mode: Causally consistent
	private static String consistencyType = "causal";
	
	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances Datacenter 1 is the local datacenter always
	 */
	private static final String dataCenter1 = "<DNS-OF-DATACENTER-1>";
	private static final String dataCenter2 = "<DNS-OF-DATACENTER-2>";
	private static final String dataCenter3 = "<DNS-OF-DATACENTER-3>";

	@Override
	public void start() {
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				final String loc = map.get("loc");
				final Long timestamp = System.currentTimeMillis();
				Thread t = new Thread(new Runnable() {
					public void run() {
						try {
							// Calculate the primary datacenter for the key
							// For simplicity, key=1 -> Datacenter1
							// key=2 -> Datacenter2
							// key=3 -> Datacenter3
							// Can be replaced with a hashing algorithm and
							// range
							if (key.equals("1")) {
								KeyValueLib.PUT(dataCenter1, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter2, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter3, key, value,
										timestamp.toString(), loc, consistencyType);
							} else if (key.equals("2")) {
								KeyValueLib.PUT(dataCenter2, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter1, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter3, key, value,
										timestamp.toString(), loc, consistencyType);
							} else if (key.equals("3")) {
								KeyValueLib.PUT(dataCenter3, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter1, key, value,
										timestamp.toString(), loc, consistencyType);
								KeyValueLib.PUT(dataCenter2, key, value,
										timestamp.toString(), loc, consistencyType);
							}
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				t.start();
				req.response().end(); // Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final Integer loc = Integer.parseInt(map.get("loc"));
				final Long timestamp = System.currentTimeMillis();
				Thread t = new Thread(new Runnable() {
					public void run() {
						try {
							String response = "0";
							switch (loc) {
							case 1:
								response = KeyValueLib.GET(dataCenter1, key,
										timestamp.toString(), consistencyType);
								break;
							case 2:
								response = KeyValueLib.GET(dataCenter2, key,
										timestamp.toString(), consistencyType);
								break;
							case 3:
								response = KeyValueLib.GET(dataCenter3, key,
										timestamp.toString(), consistencyType);
								break;
							default:
								break;
							}
							// TODO: If response format changes, parse it
							req.response().end(response);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/consistency", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				consistencyType = map.get("consistency");
				req.response().end();
			}
		});

		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}