package mil.nga.giat.geowave.types.gpx;

import java.io.File;
import java.io.IOException;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.store.CloseableIterator;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GPXIngestPluginTest {

    @Test
    public void test() throws IOException {
        GpxIngestPlugin pluggin = new GpxIngestPlugin();
        pluggin.init(new File(this.getClass().getClassLoader().getResource("metadata.xml").getPath()).getParentFile());

        CloseableIterator<GeoWaveData<SimpleFeature>> consumer = pluggin.toGeoWaveData(new File(this.getClass().getClassLoader().getResource("12345.xml").getPath()), new ByteArrayId("123".getBytes()), "");

        int totalCount = 0;
        while (consumer.hasNext()) {
            System.out.println(consumer.next().getValue().toString());
            totalCount++;
        }
        consumer.close();
        assertEquals(9, totalCount);
    }
}
