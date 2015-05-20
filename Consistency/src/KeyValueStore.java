import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;

import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class KeyValueStore extends Verticle {
	private HashMap<String, ArrayList<StoreValue>> store = null;
	private static ReentrantLock lock = new ReentrantLock();

	public KeyValueStore() {
		store = new HashMap<String, ArrayList<StoreValue>>();
	}

	private static synchronized void PUT_eventual(KeyValueStore keyValueStore,
			String key, StoreValue value) {
		ArrayList<StoreValue> storeValues = keyValueStore.store.get(key);
		if (storeValues == null) {
			storeValues = new ArrayList<StoreValue>();
		}
		storeValues.add(value);
	}

	private static synchronized void PUT(KeyValueStore keyValueStore,
			String key, StoreValue value) {
		ArrayList<StoreValue> storeValues = keyValueStore.store.get(key);
		if (storeValues == null) {
			storeValues = new ArrayList<StoreValue>();
			storeValues.add(value);
		} else {
			int index = 0;
			int flag = 0;
			for (StoreValue sv : storeValues) {
				if (value.getTimestamp() < sv.getTimestamp()) {
					storeValues.add(index, value);
					flag = 1;
					break;
				}
				index++;
			}
			if (flag == 0)
				storeValues.add(value);
		}
		keyValueStore.store.put(key, storeValues);
	}

	private static synchronized ArrayList<StoreValue> GET(
			KeyValueStore keyValueStore, String key) {
		return keyValueStore.store.get(key);
	}

	private static long handleSkew(long timestamp, int region) {
		long adjustedTimestamp = 0;
		// TODO: Add skews here
		// Skews for US-EAST
		switch (region) {
		case Constants.US_EAST:
			adjustedTimestamp = timestamp - 5;
			break;
		case Constants.US_WEST:
			adjustedTimestamp = timestamp - 200 + 10800000;
			break;
		case Constants.SINGAPORE:
			adjustedTimestamp = timestamp - 800 - 43200000;
			break;
		default:
			break;
		}

		// Skews for US-WEST
		switch (region) {
		case Constants.US_EAST:
			adjustedTimestamp = timestamp - 200 - 10800000;
			break;
		case Constants.US_WEST:
			adjustedTimestamp = timestamp - 5;
			break;
		case Constants.SINGAPORE:
			adjustedTimestamp = timestamp - 800 - 54000000;
			break;
		default:
			break;
		}

		// Skews for Singapore
		switch (region) {
		case Constants.US_EAST:
			adjustedTimestamp = timestamp - 800 + 43200000;
			break;
		case Constants.US_WEST:
			adjustedTimestamp = timestamp - 800 + 54000000;
			break;
		case Constants.SINGAPORE:
			adjustedTimestamp = timestamp - 5;
			break;
		default:
			break;
		}

		return adjustedTimestamp;
	}

	@Override
	public void start() {
		final KeyValueStore keyValueStore = new KeyValueStore();
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);
		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				String value = map.get("value");
				String consistency = map.get("consistency");
				if (consistency.equals("strong"))
					lock.lock();
				Long timestamp = Long.parseLong(map.get("timestamp"));
				Integer region = Integer.parseInt(map.get("region"));
				System.out.println("PUT request received for key: " + key
						+ " and value: " + value + " at timestamp: "
						+ timestamp);

				Long adjustedTimestamp = handleSkew(timestamp, region);

				// Prepare StoreValue object.
				StoreValue sv = new StoreValue(adjustedTimestamp, value);

				if(consistency.equals("eventual"))
					PUT_eventual(keyValueStore, key, sv);
				else
					PUT(keyValueStore, key, sv);
				String response = "stored";
				req.response().putHeader("Content-Type", "text/plain");
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
				if (consistency.equals("strong"))
					lock.unlock();
			}
		});
		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				String key = map.get("key");
				System.out.print("GET request received for key: " + key
						+ " at ");
				System.out.println(new Timestamp(System.currentTimeMillis()
						+ TimeZone.getTimeZone("EST").getRawOffset()));
				ArrayList<StoreValue> values = GET(keyValueStore, key);

				// TODO: Prepare response. Json would be good.
				// For now, the response is converted to String

				String response = "";

				if (values != null) {
					for (StoreValue val : values) {
						response = response + val.getValue() + " ";
					}
				}

				req.response().putHeader("Content-Type", "text/plain");
				if (response != null)
					req.response().putHeader("Content-Length",
							String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
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