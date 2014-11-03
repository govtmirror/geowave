package mil.nga.giat.geowave.types.gpx;

import com.vividsolutions.jts.geom.Coordinate;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.GeometryUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Consumes a GPX file.  
 * The consumer  is an iterator, parsing the input stream and returning results as the stream is parsed.
 * Data is emitted for each element at the 'end' tag.
 * 
 * Caution: Developers should maintain the cohesiveness of attribute names associated with each feature type defined in {@link GpxUtils}.
 * 
 * Route way points and way points are treated similarly except way points do not include the parent ID information in their ID.  The assumption is
 * that the name, lat and lon attributes are globally unique.  In contrast, Route way points include the file name and parent route name as part of their ID.
 * Routes are not assumed to be global.
 * 
 * 
 */
public class GPXConsumer implements
        CloseableIterator<GeoWaveData<SimpleFeature>> {

    private final static Logger LOGGER = Logger.getLogger(GpxIngestPlugin.class);

    private final SimpleFeatureBuilder pointBuilder;
    private final SimpleFeatureBuilder waypointBuilder;
    private final SimpleFeatureBuilder routeBuilder;
    private final SimpleFeatureBuilder trackBuilder;
    protected static final SimpleFeatureType pointType = GpxUtils.createGPXPointDataType();
    protected static final SimpleFeatureType waypointType = GpxUtils.createGPXWaypointDataType();
    
    protected static final SimpleFeatureType trackType = GpxUtils.createGPXTrackDataType();
    protected static final SimpleFeatureType routeType = GpxUtils.createGPXRouteDataType();

    protected static final ByteArrayId pointKey = new ByteArrayId(
            StringUtils.stringToBinary(GpxUtils.GPX_POINT_FEATURE));
    protected static final ByteArrayId waypointKey = new ByteArrayId(
            StringUtils.stringToBinary(GpxUtils.GPX_WAYPOINT_FEATURE));
    protected static final ByteArrayId trackKey = new ByteArrayId(
            StringUtils.stringToBinary(GpxUtils.GPX_TRACK_FEATURE));
    protected static final ByteArrayId routeKey = new ByteArrayId(
            StringUtils.stringToBinary(GpxUtils.GPX_ROUTE_FEATURE));

    final InputStream fileStream;
    final ByteArrayId primaryIndexId;
    final String inputID;
    final String globalVisibility;
    final Map<String, Map<String, String>> additionalData;

    final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

    final Stack<GPXDataElement> currentElementStack = new Stack<GPXDataElement>();
    final GPXDataElement top = new GPXDataElement(
            "gpx");

    XMLEventReader eventReader;
    GeoWaveData<SimpleFeature> nextFeature = null;
    Coordinate nextCoordinate = null;

    public GPXConsumer(
            InputStream fileStream,
            ByteArrayId primaryIndexId,
            String inputID,
            Map<String, Map<String, String>> additionalData,
            String globalVisibility) {
        super();
        this.fileStream = fileStream;
        this.primaryIndexId = primaryIndexId;
        this.inputID = inputID;
        this.additionalData = additionalData;
        this.globalVisibility = globalVisibility;
        pointBuilder = new SimpleFeatureBuilder(
                pointType);
        waypointBuilder = new SimpleFeatureBuilder(
                waypointType);
        trackBuilder = new SimpleFeatureBuilder(
                trackType);
        routeBuilder = new SimpleFeatureBuilder(
                routeType);
        try {
            eventReader = inputFactory.createXMLEventReader(fileStream);
            init();
            nextFeature = getNext();
        } catch (IOException | XMLStreamException e) {
            LOGGER.error(
                    "Error processing GPX input stream",
                    e);
            nextFeature = null;
        }
    }

    @Override
    public boolean hasNext() {
        return (nextFeature != null);
    }

    @Override
    public GeoWaveData<SimpleFeature> next() {
        GeoWaveData<SimpleFeature> ret = nextFeature;
        try {
            nextFeature = getNext();
        } catch (XMLStreamException e) {
            LOGGER.error(
                    "Error processing GPX input stream",
                    e);
            nextFeature = null;
        }
        return ret;
    }

    @Override
    public void remove() {
    }

    @Override
    public void close()
            throws IOException {
        try {
            eventReader.close();
        } catch (final Exception e2) {
            LOGGER.warn(
                    "Unable to close track XML stream",
                    e2);
        }
        IOUtils.closeQuietly(fileStream);

    }

    private void init()
            throws IOException,
            XMLStreamException {

        while (eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();
            if (event.isStartElement()) {
                StartElement node = event.asStartElement();
                if ("gpx".equals(node.getName().getLocalPart())) {
                    currentElementStack.push(top);
                    processElementAttributes(node,
                            top);
                    return;

                }
            }
        }
    }

    private GeoWaveData<SimpleFeature> getNext()
            throws
            XMLStreamException {

        GPXDataElement currentElement = currentElementStack.peek();
        GeoWaveData<SimpleFeature> newFeature = null;
        while (newFeature == null && eventReader.hasNext()) {
            XMLEvent event = eventReader.nextEvent();
            if (event.isStartElement()) {
                StartElement node = event.asStartElement();
                if (!processElementValues(
                        node,
                        currentElement)) {
                    GPXDataElement newElement = new GPXDataElement(event.asStartElement().getName().getLocalPart());
                    currentElement.addChild(newElement);
                    currentElement = newElement;
                    currentElementStack.push(currentElement);
                    processElementAttributes(node, currentElement);
                }
            } else if (event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
                    currentElement.elementType)) {
                GPXDataElement child = currentElementStack.pop();
                newFeature = postProcess(child);
                if (newFeature == null && !currentElementStack.isEmpty()) {
                    currentElement = currentElementStack.peek();
                 //   currentElement.removeChild(child);
                }
            }
        }
        return newFeature;
    }

    private String getChildCharacters(
            XMLEventReader eventReader,
            String elType)
            throws XMLStreamException {
        StringBuilder buf = new StringBuilder();
        XMLEvent event = eventReader.nextEvent();
        while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
                elType))) {
            if (event.isCharacters()) {
                buf.append(event.asCharacters().getData());
            }
            event = eventReader.nextEvent();
        }
        return buf.toString().trim();

    }

    private void processElementAttributes(
            StartElement node,
            GPXDataElement element)
            throws NumberFormatException,
            XMLStreamException {
        final Iterator<Attribute> attributes = node.getAttributes();
        while (attributes.hasNext()) {
            final Attribute a = attributes.next();
            if (a.getName().getLocalPart().equals(
                    "lon")) {
                element.lon = Double.parseDouble(a.getValue());
            } else if (a.getName().getLocalPart().equals(
                    "lat")) {
                element.lat = Double.parseDouble(a.getValue());
            }
        }
    }

    private boolean processElementValues(
            StartElement node,
            GPXDataElement element)
            throws NumberFormatException,
            XMLStreamException {
        switch (node.getName().getLocalPart()) {
            case "ele": {
                element.elevation = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "ele"));
                break;
            }
            case "magvar": {
                element.magvar = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "magvar"));
                break;
            }
            case "geoidheight": {
                element.geoidheight = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "geoidheight"));
                break;
            }
            case "name": {
                element.name = getChildCharacters(
                        eventReader,
                        "name");
                break;
            }
            case "cmt": {
                element.cmt = getChildCharacters(
                        eventReader,
                        "cmt");
                break;
            }
            case "desc": {
                element.desc = getChildCharacters(
                        eventReader,
                        "desc");
                break;
            }
            case "src": {
                element.src = getChildCharacters(
                        eventReader,
                        "src");
                break;
            }
            case "link": {
                element.link = getChildCharacters(
                        eventReader,
                        "link");
                break;
            }
            case "sym": {
                element.sym = getChildCharacters(
                        eventReader,
                        "sym");
                break;
            }
            case "type": {
                element.type = getChildCharacters(
                        eventReader,
                        "type");
                break;
            }
            case "sat": {
                element.sat = Long.parseLong(getChildCharacters(
                        eventReader,
                        "sat"));
                break;
            }
            case "vdop": {
                element.vdop = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "vdop"));
                break;
            }
            case "hdop": {
                element.hdop = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "hdop"));
                break;
            }
            case "pdop": {
                element.pdop = Double.parseDouble(getChildCharacters(
                        eventReader,
                        "pdop"));
                break;
            }
            case "time": {
                try {
                    element.timestamp = GpxUtils.TIME_FORMAT_SECONDS.parse(
                            getChildCharacters(
                                    eventReader,
                                    "time")).getTime();

                } catch (final Exception t) {
                    try {
                        element.timestamp = GpxUtils.TIME_FORMAT_MILLIS.parse(
                                getChildCharacters(
                                        eventReader,
                                        "time")).getTime();
                    } catch (final Exception t2) {

                    }
                }
                break;
            }
            default:
                return false;
        }
        return true;

    }

    private static class GPXDataElement {

        Long timestamp = null;
        Double elevation = null;
        Double lat = null;
        Double lon = null;
        Double magvar = null;
        Double geoidheight = null;
        String name = null;
        String cmt = null;
        String desc = null;
        String src = null;
        String link = null;
        String sym = null;
        String type = null;
        Long sat = null;
        Double hdop = null;
        Double pdop = null;
        Double vdop = null;
        String elementType;

        Coordinate coordinate = null;
        List<GPXDataElement> children = null;
        GPXDataElement parent;
        long id = 0;

        public GPXDataElement(
                String myElType) {
            this.elementType = myElType;
        }

        public String toString() {
            return elementType;
        }

        public String getPath() {
            StringBuffer buf = new StringBuffer();
            GPXDataElement currentGP = parent;
            buf.append(this.elementType);
            while (currentGP != null) {
                buf.insert(0, '.');
                buf.insert(0, currentGP.elementType);
                currentGP = currentGP.parent;
            }
            return buf.toString();
        }

        public void addChild(
                GPXDataElement child) {

            if (children == null) {
                children = new ArrayList<GPXDataElement>();
            }
            children.add(child);
            child.parent = this;
            child.id = children.size();
        }

        public void removeChild(
                GPXDataElement child) {

            if (children == null) {
                return;
            }
            children.remove(child);

        }

        public String composeID(String prefix, boolean includeLatLong) {
            StringBuffer buf = new StringBuffer();
            buf.append(prefix);
            if (prefix.length() > 0) {
                buf.append('_');
            }
            if (parent != null) {
                String parentID = parent.composeID("", false);
                if (parentID.length() > 0) {
                    buf.append(parentID);
                    buf.append('_');
                }
                buf.append(id);
                buf.append('_');
            }
            if (name != null && name.length() > 0) {
                buf.append(name.replaceAll("\\s+", "_"));
                buf.append('_');
            }
            if (includeLatLong && lat != null && lon != null) {
                buf.append(lat.hashCode()).append('_').append(lon.hashCode());
                buf.append('_');
            }
            buf.deleteCharAt(buf.length() - 1);
            return buf.toString();
        }

        public Coordinate getCoordinate() {
            if (coordinate != null) {
                return coordinate;
            }
            if (lat != null && lon != null) {
                coordinate = new Coordinate(
                        lon,
                        lat);
            }
            return coordinate;
        }

        public boolean isCoordinate() {
            return this.lat != null && this.lon != null;
        }

        public List<Coordinate> buildCoordinates() {
            if (isCoordinate()) {
                return Arrays.asList(getCoordinate());
            }
            ArrayList<Coordinate> coords = new ArrayList<Coordinate>();
            for (int i = 0; children != null && i < this.children.size(); i++) {
                coords.addAll(children.get(i).buildCoordinates());
            }
            return coords;
        }

        private Long getStartTime() {
            if (children == null) {
                return timestamp;
            }
            long minTime = Long.MAX_VALUE;
            for (GPXDataElement element : children) {
                Long t = element.getStartTime();
                if (t != null) {
                    minTime = Math.min(t.longValue(), minTime);
                }
            }
            return (minTime < Long.MAX_VALUE) ? new Long(minTime) : null;
        }

        private Long getEndTime() {
            if (children == null) {
                return timestamp;
            }
            long maxTime = 0;
            for (GPXDataElement element : children) {
                Long t = element.getEndTime();
                if (t != null) {
                    maxTime = Math.max(t.longValue(), maxTime);
                }
            }
            return (maxTime > 0) ? new Long(maxTime) : null;
        }

        public boolean build(
                SimpleFeatureBuilder waypointBuilder) {
            if ((lon != null) && (lat != null)) {
                final Coordinate p = getCoordinate();
                waypointBuilder.set(
                        "geometry",
                        GeometryUtils.GEOMETRY_FACTORY.createPoint(p));
                waypointBuilder.set(
                        "Latitude",
                        lat);
                waypointBuilder.set(
                        "Longitude",
                        lon);
            }
            if (elevation != null) {
                waypointBuilder.set(
                        "Elevation",
                        elevation);
            }
            if (name != null) {
                waypointBuilder.set(
                        "Name",
                        name);
            }
            if (cmt != null) {
                waypointBuilder.set(
                        "Comment",
                        cmt);
            }
            if (desc != null) {
                waypointBuilder.set(
                        "Description",
                        desc);
            }
            if (sym != null) {
                waypointBuilder.set(
                        "Symbol",
                        sym);
            }
            if (timestamp != null) {
                waypointBuilder.set(
                        "Timestamp",
                        new Date(
                                timestamp));
            }
            if (this.children != null) {

                boolean setDuration = true;
                
                List<Coordinate> childSequence = buildCoordinates();
                
                if (childSequence.size() == 0) return false;
                  
                if (childSequence.size() > 1) {
                    waypointBuilder.set(
                            "geometry",
                            GeometryUtils.GEOMETRY_FACTORY.createLineString(childSequence.toArray(new Coordinate[childSequence.size()])));
                } else {
                    waypointBuilder.set(
                            "geometry",
                            GeometryUtils.GEOMETRY_FACTORY.createPoint(childSequence.get(0)));
                }
                waypointBuilder.set(
                        "NumberPoints",
                        childSequence.size());
                              

                Long minTime = getStartTime();
                if (minTime != null) {
                    waypointBuilder.set(
                            "StartTimeStamp",
                            new Date(
                                    minTime));
                } else {
                    setDuration = false;
                }
                Long maxTime = getEndTime();
                if (maxTime != null) {
                    waypointBuilder.set(
                            "EndTimeStamp",
                            new Date(
                                    maxTime));
                } else {
                    setDuration = false;
                }
                if (setDuration) {
                    waypointBuilder.set(
                            "Duration",
                            maxTime - minTime);
                } 
            }
            return true;
        }
    }

    private GeoWaveData<SimpleFeature> postProcess(
            GPXDataElement element) {

        switch (element.elementType) {
            case "trk": {
                if (element.children != null && element.build(trackBuilder)) {
                    trackBuilder.set(
                            "TrackId",
                            inputID);
                    return buildGeoWaveDataInstance( element.composeID(inputID, false), primaryIndexId, trackKey, trackBuilder, additionalData.get(element.getPath()));
                }
                break;
            }
            case "rte": {

                if (element.children != null && element.build(routeBuilder)) {
                    trackBuilder.set(
                            "TrackId",
                            inputID);
                    return buildGeoWaveDataInstance( element.composeID(inputID, false), primaryIndexId, routeKey, routeBuilder, additionalData.get(element.getPath()));
                }
                break;
            }
            case "wpt": {

                if (element.build(waypointBuilder)) {
                    return buildGeoWaveDataInstance( element.composeID("", true), primaryIndexId, waypointKey, waypointBuilder, additionalData.get(element.getPath()));
                }
                break;
            }
            case "rtept": {
                if (element.build(waypointBuilder)) {
                    return buildGeoWaveDataInstance( element.composeID(inputID, true), primaryIndexId, waypointKey, waypointBuilder, additionalData.get(element.getPath()));
                }
                break;
            }
            case "trkseg": {
                break;
            }
            case "trkpt": {

                if (element.build(pointBuilder)) {
                    if (element.timestamp == null) {
                        pointBuilder.set(
                                "Timestamp",
                                null);
                    }
                    return buildGeoWaveDataInstance( element.composeID(inputID, false), primaryIndexId, pointKey, pointBuilder, additionalData.get(element.getPath()));
                }
                break;
            }
        }
        return null;
    }

    private static GeoWaveData<SimpleFeature> buildGeoWaveDataInstance(String id,
            ByteArrayId primaryIndexId, ByteArrayId key, SimpleFeatureBuilder builder, Map<String, String> additionalDataSet) {

        if (additionalDataSet != null) {
            for (Map.Entry<String, String> entry : additionalDataSet.entrySet()) {
                builder.set(entry.getKey(), entry.getValue());
            }
        }
        return new GeoWaveData<SimpleFeature>(
                key,
                primaryIndexId,
                builder.buildFeature(id));
    }
}
