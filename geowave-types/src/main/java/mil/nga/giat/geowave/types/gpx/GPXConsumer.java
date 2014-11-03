package mil.nga.giat.geowave.types.gpx;

import com.vividsolutions.jts.geom.Coordinate;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

public class GPXConsumer implements
		CloseableIterator<GeoWaveData<SimpleFeature>>
{

	private final static Logger LOGGER = Logger.getLogger(GpxIngestPlugin.class);

	private final SimpleFeatureBuilder pointBuilder;
	private final SimpleFeatureBuilder waypointBuilder;
	private final SimpleFeatureBuilder routeBuilder;
	private final SimpleFeatureBuilder trackBuilder;
	private final SimpleFeatureType pointType = GpxUtils.createGPXPointDataType();
	private final SimpleFeatureType waypointType = GpxUtils.createGPXWaypointDataType();;
	private final SimpleFeatureType trackType = GpxUtils.createGPXTrackDataType();
        private final SimpleFeatureType routeType = GpxUtils.createGPXRouteDataType();

	private final ByteArrayId pointKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_POINT_FEATURE));
	private final ByteArrayId waypointKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_WAYPOINT_FEATURE));
	private final ByteArrayId trackKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_TRACK_FEATURE));
        private final ByteArrayId routeKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_ROUTE_FEATURE));
        

	final InputStream fileStream;
	final ByteArrayId primaryIndexId;
	final String inputID;
	final String globalVisibility;
        final Map<String,Map<String,String>> additionalData;

	final XMLInputFactory inputFactory = XMLInputFactory.newInstance();

	final Stack<WayPoint> currentPointStack = new Stack<WayPoint>();
	final WayPoint top = new WayPoint(
			"gpx");

	XMLEventReader eventReader;
	GeoWaveData<SimpleFeature> nextFeature = null;
	Coordinate nextCoordinate = null;
        

	public GPXConsumer(
			InputStream fileStream,
			ByteArrayId primaryIndexId,
			String inputID,
                        Map<String,Map<String,String>> additionalData,
			String globalVisibility ) {
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
		}
		catch (IOException | XMLStreamException e) {
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
		}
		catch (XMLStreamException e) {
			LOGGER.error(
					"Error processing GPX input stream",
					e);
			nextFeature = null;
		}
		return ret;
	}

	@Override
	public void remove() {}

	@Override
	public void close()
			throws IOException {
		try {
			eventReader.close();
		}
		catch (final Exception e2) {
			LOGGER.warn(
					"Unable to close track XML stream",
					e2);
		}
		IOUtils.closeQuietly(fileStream);

	}
        
        
	private void init()
			throws IOException,
			XMLStreamException {

		while (eventReader.hasNext() ) {
			XMLEvent event = eventReader.nextEvent();
			if (event.isStartElement()) {
				StartElement node = event.asStartElement();
				if ("gpx".equals(node.getName().getLocalPart())) {
						currentPointStack.push(top);
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

                WayPoint currentPoint = currentPointStack.peek();
                GeoWaveData<SimpleFeature> newFeature = null;
		while (newFeature == null && eventReader.hasNext() ) {
			XMLEvent event = eventReader.nextEvent(); 
			if (event.isStartElement()) {		
                             StartElement node = event.asStartElement();                            
                             if (!processElementValues(
				node,
				currentPoint)) {
                                     WayPoint  newPoint = new WayPoint( event.asStartElement().getName().getLocalPart());
                                     currentPoint.addChild(newPoint);
                                     currentPoint = newPoint;
                                      currentPointStack.push(currentPoint);  
                                     processElementAttributes(node, currentPoint);                                                                               
                             }			 			
			}
                        else if (event.isEndElement() &&  event.asEndElement().getName().getLocalPart().equals(
				currentPoint.elementType)) {
                            WayPoint child =  currentPointStack.pop();
                            newFeature = postProcess(child);  
                            if (newFeature == null && !currentPointStack.isEmpty()) {
                                currentPoint =currentPointStack.peek();
                                currentPoint.removeChild(child);
                            }
                        }
		}
		return newFeature;
	}

	private String getChildCharacters(
			XMLEventReader eventReader,
			String elType )
			throws XMLStreamException {
		StringBuffer buf = new StringBuffer();
		XMLEvent event = eventReader.nextEvent();
		while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
				elType))) {
			if (event.isCharacters()) buf.append(event.asCharacters().getData());
			event = eventReader.nextEvent();
		}
		return buf.toString().trim();

	}

	private void processElementAttributes(
			StartElement node,
			WayPoint point )
			throws NumberFormatException,
			XMLStreamException {
		final Iterator<Attribute> attributes = node.getAttributes();
		while (attributes.hasNext()) {
			final Attribute a = attributes.next();
			if (a.getName().getLocalPart().equals(
					"lon")) {
				point.lon = Double.parseDouble(a.getValue());
			}
			else if (a.getName().getLocalPart().equals(
					"lat")) {
				point.lat = Double.parseDouble(a.getValue());
			}
		}
	}

	private boolean processElementValues(
			StartElement node,
			WayPoint point )
			throws NumberFormatException,
			XMLStreamException {
		switch (node.getName().getLocalPart()) {
			case "ele": {
				point.elevation = Double.parseDouble(getChildCharacters(
						eventReader,
						"ele"));
				break;
			}
			case "magvar": {
				point.magvar = Double.parseDouble(getChildCharacters(
						eventReader,
						"magvar"));
				break;
			}
			case "geoidheight": {
				point.geoidheight = Double.parseDouble(getChildCharacters(
						eventReader,
						"geoidheight"));
				break;
			}
			case "name": {
				point.name = getChildCharacters(
						eventReader,
						"name");
				break;
			}
			case "cmt": {
				point.cmt = getChildCharacters(
						eventReader,
						"cmt");
				break;
			}
			case "desc": {
				point.desc = getChildCharacters(
						eventReader,
						"desc");
				break;
			}
			case "src": {
				point.src = getChildCharacters(
						eventReader,
						"src");
				break;
			}
			case "link": {
				point.link = getChildCharacters(
						eventReader,
						"link");
				break;
			}
			case "sym": {
				point.sym = getChildCharacters(
						eventReader,
						"sym");
				break;
			}
			case "type": {
				point.type = getChildCharacters(
						eventReader,
						"type");
				break;
			}
			case "sat": {
				point.sat = Long.parseLong(getChildCharacters(
						eventReader,
						"sat"));
				break;
			}
			case "vdop": {
				point.vdop = Double.parseDouble(getChildCharacters(
						eventReader,
						"vdop"));
				break;
			}
			case "hdop": {
				point.hdop = Double.parseDouble(getChildCharacters(
						eventReader,
						"hdop"));
				break;
			}
			case "pdop": {
				point.pdop = Double.parseDouble(getChildCharacters(
						eventReader,
						"pdop"));
				break;
			}
			case "time": {
				try {
					point.timestamp = GpxUtils.TIME_FORMAT_SECONDS.parse(
							getChildCharacters(
									eventReader,
									"time")).getTime();

				}
				catch (final Exception t) {
					try {
						point.timestamp = GpxUtils.TIME_FORMAT_MILLIS.parse(
								getChildCharacters(
										eventReader,
										"time")).getTime();
					}
					catch (final Exception t2) {

					}
				}
				break;
			}
			default:
				return false;
		}
		return true;

	}

	private static class WayPoint
	{
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
		List<WayPoint> children = null;
                WayPoint parent;
                long id  = 0;

		public WayPoint(
				String myElType ) {
			this.elementType = myElType;
		}

		public String toString() {
			return elementType;
		}

                public String getPath() {
                    StringBuffer buf = new StringBuffer();
                    WayPoint currentGP = parent;
                    buf.append(this.elementType);
                    while(currentGP != null) {         
                        buf.insert(0,'.');
                        buf.insert(0,currentGP.elementType);
                        currentGP=currentGP.parent;
                    }
                    return buf.toString();
                }
                
		public void addChild(
				WayPoint child ) {
                    
			if (children == null) {
				children = new ArrayList<WayPoint>();
			}
			children.add(child);
                        child.parent = this;
                        child.id = children.size();
		}
                public void removeChild(
				WayPoint child ) {
                    
			if (children == null) {
				return;
			}
                        children.remove(child);


		}
                
                public String composeID(String prefix, boolean includeLatLong) {
                    StringBuffer buf = new StringBuffer();
                    buf.append(prefix);
                    if (prefix.length() > 0) buf.append('_');
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
                         buf.append(name);
                         buf.append('_');
                    }
                    if (includeLatLong && lat != null && lon != null) {                       
                         buf.append(lat.hashCode()).append('_').append(lon.hashCode());
                         buf.append('_');
                    }
                    buf.deleteCharAt(buf.length()-1);
                    return buf.toString();
                }

		public Coordinate getCoordinate() {
			if (coordinate != null) return coordinate;
			if (lat != null && lon != null) coordinate = new Coordinate(
					lon,
					lat);
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
			for (int i = 0; i < this.children.size(); i++) {
                            coords.addAll(children.get(i).buildCoordinates());
			}
			return coords;
		}

		private Long getStartTime() {
                    if (children == null) return timestamp;
                    long minTime = Long.MAX_VALUE;
                    for (WayPoint point : children) {
                        Long t = point.getStartTime();
                        if (t != null)
                            minTime = Math.min(t.longValue(), minTime);
                    }
                    return (minTime < Long.MAX_VALUE) ? new Long(minTime) : null;
		}
                
                private Long getEndTime() {
                    if (children == null) return timestamp;
                    long maxTime = 0;
                    for (WayPoint point : children) {
                        Long t = point.getEndTime();
                        if (t != null)
                            maxTime = Math.max(t.longValue(), maxTime);
                    }
                    return (maxTime > 0) ? new Long(maxTime) : null;
		}

		public boolean build(
				SimpleFeatureBuilder waypointBuilder ) {
                    System.out.println("building " + toString());
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
				if (elevation != null) waypointBuilder.set(
						"Elevation",
						elevation);
				if (name != null) waypointBuilder.set(
						"Name",
						name);
				if (cmt != null) waypointBuilder.set(
						"Comment",
						cmt);
				if (desc != null) waypointBuilder.set(
						"Description",
						desc);
				if (sym != null) waypointBuilder.set(
						"Symbol",
						sym);
				if (timestamp != null) {
					waypointBuilder.set(
							"Timestamp",
							new Date(
									timestamp));
				}
                                if (this.children != null && this.children.size() > 0) {
                                    
                                    boolean setDuration = true;
                                
                                    List<Coordinate> childSequence = buildCoordinates();
                                    if (childSequence.size() > 1) {
					waypointBuilder.set(
							"geometry",
							GeometryUtils.GEOMETRY_FACTORY.createLineString(childSequence.toArray(new Coordinate[childSequence.size()])));
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
				}
				else {
					setDuration = false;

					waypointBuilder.set(
							"StartTimeStamp",
							null);
				}
                                     Long maxTime = getStartTime();
				if (maxTime != null) {
					waypointBuilder.set(
							"EndTimeStamp",
							new Date(
									maxTime));
				}
				else {
					setDuration = false;

					waypointBuilder.set(
							"EndTimeStamp",
							null);
				}
				if (setDuration) {
					waypointBuilder.set(
							"Duration",
							maxTime - minTime);
				}
				else {
					waypointBuilder.set(
							"Duration",
							null);
				}
                         }
          return true;
		}
	}
        
	private GeoWaveData<SimpleFeature> postProcess(
			WayPoint point ) {

		switch (point.elementType) {
			case "trk": {	                    
                            if (point.build(trackBuilder)) {
				trackBuilder.set(
						"TrackId",
						inputID);
                                
                                Map<String,String> dataSet = additionalData.get(point.getPath());
                                if (dataSet != null) {
                                    for (Map.Entry<String,String> entry : dataSet.entrySet()) {
                                        trackBuilder.set(entry.getKey(), entry.getValue());
                                    }
                                }
				/*
				 * trackBuilder.set( "UserId", gpxTrack.getUserid());
				 * trackBuilder.set( "User", gpxTrack.getUser());
				 * trackBuilder.set( "Description", gpxTrack.getDescription());
				 * 
				 * if ((gpxTrack.getTags() != null) &&
				 * (gpxTrack.getTags().size() > 0)) { final String tags =
				 * org.apache.commons.lang.StringUtils.join( gpxTrack.getTags(),
				 * TAG_SEPARATOR); trackBuilder.set( "Tags", tags); } else {
				 * trackBuilder.set( "Tags", null); }
                                
                                	trackBuilder.set(
					"NumberPoints",
					trackPoint);
			trackBuilder.set(
					"TrackId",
					gpxTrack.getTrackid().toString());
			trackBuilder.set(
					"UserId",
					gpxTrack.getUserid());
			trackBuilder.set(
					"User",
					gpxTrack.getUser());
			trackBuilder.set(
					"Description",
					gpxTrack.getDescription());

			if ((gpxTrack.getTags() != null) && (gpxTrack.getTags().size() > 0)) {
				final String tags = org.apache.commons.lang.StringUtils.join(
						gpxTrack.getTags(),
						TAG_SEPARATOR);
				trackBuilder.set(
						"Tags",
						tags);
			}
			else {
				trackBuilder.set(
						"Tags",
						null);
			}
				 */
				return new GeoWaveData<SimpleFeature>(
						trackKey,
						primaryIndexId,
						trackBuilder.buildFeature(point.composeID(inputID, false)));
                            }
                            break;
			}
			case "rte": {
                  
				if (point.build(routeBuilder)) {
                                    	trackBuilder.set(
						"TrackId",
						inputID);
					return new GeoWaveData<SimpleFeature>(
							routeKey,
							primaryIndexId,
							routeBuilder.buildFeature(point.composeID(inputID, false)));

				}
				break;
			}
			case "wpt": {
                         
				if (point.build(waypointBuilder)) {
					return new GeoWaveData<SimpleFeature>(
							waypointKey,
							primaryIndexId,
							waypointBuilder.buildFeature(point.composeID("", true)));

				}
				break;
			}
			case "rtept": { 
				if (point.build(waypointBuilder)) {

					return new GeoWaveData<SimpleFeature>(
							waypointKey,
							primaryIndexId,
							waypointBuilder.buildFeature(point.composeID(inputID, true)));

				}
				break;
			}
                        case "trkseg" : {
                               break;
                        }
			case "trkpt": {
                       
				if (point.build(pointBuilder)) {
					if (point.timestamp == null) {
						pointBuilder.set(
								"Timestamp",
								null);
					}
					return new GeoWaveData<SimpleFeature>(
							pointKey,
							primaryIndexId,
							pointBuilder.buildFeature(point.composeID(inputID, false)));
				}

				break;
			}
		}
		return null;
	}

}