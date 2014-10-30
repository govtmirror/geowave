package mil.nga.giat.geowave.types.gpx;

import java.io.InputStream;

import org.junit.Test;

public class GPXConsumerTest
{

	@Test
	public void test() {
		InputStream is = this.getClass().getClassLoader().getResourceAsStream("sample_gpx.xml");
	}
}
