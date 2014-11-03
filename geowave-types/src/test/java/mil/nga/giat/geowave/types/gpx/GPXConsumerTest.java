package mil.nga.giat.geowave.types.gpx;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.types.HelperClass;
import mil.nga.giat.geowave.types.TestThis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;

public class GPXConsumerTest {

    Map<String, TestThis> expectedResults = new HashMap<String, TestThis>();

    @Before
    public void setup() {

        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_8_A_track_1_1", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Elevation").toString().equals("4.46")
                        && feature.getAttribute("Timestamp") != null
                        && feature.getAttribute("Latitude") != null
                        && feature.getAttribute("Longitude") != null;
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_8_A_track_1_2", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Elevation").toString().equals("4.634")
                        && feature.getAttribute("Timestamp") != null
                        && feature.getAttribute("Latitude") != null
                        && feature.getAttribute("Longitude") != null;
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_9_B_track_2_1", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Elevation").toString().equals("10.46")
                        && feature.getAttribute("Timestamp") != null
                        && feature.getAttribute("Latitude") != null
                        && feature.getAttribute("Longitude") != null;
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_9_B_track_2_2", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Elevation").toString().equals("11.634")
                        && feature.getAttribute("Timestamp") != null
                        && feature.getAttribute("Latitude") != null
                        && feature.getAttribute("Longitude") != null;
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_8_A_track", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Duration").toString().equals("60000")
                        && feature.getAttribute("StartTimeStamp") != null
                        && feature.getAttribute("NumberPoints").toString().equals("2")
                        && feature.getAttribute("EndTimeStamp") != null;
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_9_B_track", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Duration").toString().equals("60000")
                        && feature.getAttribute("StartTimeStamp") != null
                        && feature.getAttribute("NumberPoints").toString().equals("2")
                        && feature.getAttribute("EndTimeStamp") != null;
            }
        });
        expectedResults.put("AQUADUCT_0422469500_-0714618070", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Description").toString().equals("Aquaduct")
                        && feature.getAttribute("Longitude") != null
                        && feature.getAttribute("Symbol").toString().equals("Dam")
                        && feature.getAttribute("Latitude") != null;
            }
        });
        expectedResults.put("TRANSITION_0422446460_-0714685390", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Name").toString().equals("TRANSITION")
                        && feature.getAttribute("Elevation").toString().equals("92.6592");
            }
        });
        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_14_ROUT135ASP", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Name").toString().equals("ROUT135ASP")
                        && feature.getAttribute("NumberPoints").toString().equals("2")
                        && feature.getAttribute("Description").toString().equals("Route 135 ASP");
            }
        });

        expectedResults.put("123_Rockbuster_Duathlon_at_Ashland_State_Park_14_ROUT135ASP_2_rtename2_0422446460_-0714685390", new TestThis() {
            @Override
            public boolean run(SimpleFeature feature) {
                return feature.getAttribute("Longitude") != null
                        && feature.getAttribute("Latitude") != null;
            }
        });
    }

    @Test
    public void test() throws IOException {
        Set<String> expectedSet = HelperClass.buildSet(expectedResults);

        InputStream is = this.getClass().getClassLoader().getResourceAsStream("sample_gpx.xml");
        GPXConsumer consumer = new GPXConsumer(is,
                new ByteArrayId("123".getBytes()),
                "123",
                new HashMap(),
                true,
                "");
        int totalCount = 0;
        
        while (consumer.hasNext()) {
            GeoWaveData<SimpleFeature> data = consumer.next();
         //   System.out.println(data.getValue().toString());
            expectedSet.remove(data.getValue().getID());
            TestThis tester = expectedResults.get(data.getValue().getID());
            if (tester != null) {
                assertTrue(data.getValue().toString(), tester.run(data.getValue()));
            }
            totalCount++;
        }
        consumer.close();
        assertEquals(12, totalCount);
        // did everything get validated?
        assertEquals(0, expectedSet.size());
    }

}
