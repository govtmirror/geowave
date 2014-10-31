package mil.nga.giat.geowave.types.gpx;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import mil.nga.giat.geowave.index.ByteArrayId;

import org.junit.Test;

public class GPXConsumerTest
{

	@Test
	public void test() throws IOException {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("sample_gpx.xml");
		GPXConsumer consumer = new GPXConsumer(is,
				new ByteArrayId("123".getBytes()),
				"123",
				"");
		int totalCount = 0;
		while (consumer.hasNext()) {
			System.out.println(consumer.next().getValue().toString());
			totalCount++;
		}
		consumer.close();
		assertEquals(9,totalCount);
	}
}
