package io.flatf.infra.serialization.json;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JsonWriterTest {

	@Test
	public void test() {

		Map<String, String> map = new HashMap<>();
		map.put("A", "1");
		map.put("B", "2");
		map.put("C", "11");
		map.put("D", null);
		map.put("E", null);

		System.out.println(JsonWriter.toJson(null));
		System.out.println(JsonWriter.toJsonHasNulls(null));
		System.out.println(JsonWriter.toJson(map));
		System.out.println(JsonWriter.toJsonHasNulls(map));

	}

}
