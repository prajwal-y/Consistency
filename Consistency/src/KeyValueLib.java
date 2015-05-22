import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;

public class KeyValueLib {
	public static HashMap<String, Integer> dataCenters = new HashMap<String, Integer>();

	/**
	 * URLHandler helper method
	 *
	 * @param urlString
	 * @return
	 * @throws IOException
	 */
	private static String URLHandler(String urlString) throws IOException {
		StringBuilder sb = new StringBuilder();
		try {
			URL url = new URL(urlString);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			if (conn.getResponseCode() != 200) {
				throw new IOException(conn.getResponseMessage());
			}
			BufferedReader rd = new BufferedReader(new InputStreamReader(
					conn.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				sb.append(line);
			}
			rd.close();
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		return sb.toString();
	}

	/**
	 * Method to put a value in the specified data center.
	 * 
	 * @param dataCenter
	 * @param key
	 * @param value
	 * @param timestamp
	 * @param region
	 * @throws IOException
	 */
	public static void PUT(String dataCenter, String key, String value,
			String timestamp, String region, String consistency)
			throws IOException {
		String urlString = String
				.format("http://%s:8080/put?key=%s&value=%s&timestamp=%s&region=%s&consistency=%s",
						dataCenter, key, value, timestamp, region);
		try {
			switch (dataCenters.get(dataCenter)) {
			case 1:
				break;
			case 2:
				Thread.sleep(200);
				break;
			case 3:
				Thread.sleep(800);
				break;
			default:
				break;
			}
		} catch (InterruptedException e) {
			// Do nothing
		}
		String response = URLHandler(urlString);
		if (response.equals("stored")) {
			// TODO: Log the PUT request
		} else {
			System.out.println("Some error happened");
			// TODO: Log the PUT error
		}
	}

	/**
	 * Method to get a value from the specified data center.
	 * 
	 * @param dataCenter
	 * @param key
	 * @param timestamp
	 * @param region
	 * @return
	 * @throws IOException
	 */
	public static String GET(String dataCenter, String key, String timestamp,
			String consistency) throws IOException {
		String urlString = String.format(
				"http://%s:8080/get?key=%s&timestamp=%s&consistency=%s",
				dataCenter, key, timestamp);
		String value = URLHandler(urlString);
		return value;
	}

	public static void CLEAR(String dataCenter) throws IOException {
		String urlString = String.format("http://%s:8080/clear", dataCenter);
		URLHandler(urlString);
	}

}