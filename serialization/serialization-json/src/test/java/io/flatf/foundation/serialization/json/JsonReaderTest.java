package io.flatf.foundation.serialization.json;

import org.junit.Test;

public class JsonReaderTest {

	@Test
	public void test() {
		String json = "{\"A\":\"1\",\"B\":\"2\",\"C\":\"11\",\"D\":null,\"E\":null}";

		System.out.println(JsonReader.isJsonObject(json));
		System.out.println(JsonReader.toImmutableMap(json));

	}

}
