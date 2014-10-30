package mil.nga.giat.geowave.types.gpx;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
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

import com.vividsolutions.jts.geom.Coordinate;

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

	private final ByteArrayId pointKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_POINT_FEATURE));
	private final ByteArrayId waypointKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_WAYPOINT_FEATURE));
	private final ByteArrayId trackKey = new ByteArrayId(
			StringUtils.stringToBinary(GpxUtils.GPX_TRACK_FEATURE));

	final InputStream fileStream;
	final ByteArrayId primaryIndexId;
	final Long inputID;
	final String globalVisibility;
	long trackID = 0;

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
			Long inputID,
			String globalVisibility ) {
		super();
		this.fileStream = fileStream;
		this.primaryIndexId = primaryIndexId;
		this.inputID = inputID;
		this.globalVisibility = globalVisibility;
		pointBuilder = new SimpleFeatureBuilder(
				pointType);
		waypointBuilder = new SimpleFeatureBuilder(
				waypointType);
		trackBuilder = new SimpleFeatureBuilder(
				trackType);
		routeBuilder = new SimpleFeatureBuilder(
				trackType);
		try {
			eventReader = inputFactory.createXMLEventReader(fileStream);
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
		catch (IOException | XMLStreamException e) {
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

	private GeoWaveData<SimpleFeature> getNext()
			throws IOException,
			XMLStreamException {

		while (eventReader.hasNext()) {
			XMLEvent event = eventReader.peek();
			if (event.isStartElement()) {
				StartElement node = event.asStartElement();
				switch (node.getName().getLocalPart()) {
					case "gpx": {
						currentPointStack.push(top);
						continue;
					}
					default:
						return processParentElement(eventReader);
				}
			}
		}
		return null;
	}

	private GeoWaveData<SimpleFeature> processParentElement(
			XMLEventReader eventReader )
			throws XMLStreamException {
		XMLEvent event = eventReader.peek();
		WayPoint parent = currentPointStack.peek();

		while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
				parent.elementType))) {
			if (event.isStartElement()) {
				if (!processElementAttributes(
						event,
						parent)) {
					WayPoint child = new WayPoint(
							event.asStartElement().getName().getLocalPart());
					currentPointStack.push(child);
					GeoWaveData<SimpleFeature> newFeature = processParentElement(eventReader);
					currentPointStack.pop();
					if (newFeature != null) {
						parent.addChild(child);
						return newFeature;
					}
				}
			}
			eventReader.nextEvent();
			event = eventReader.peek();
		}
		return postProcess(parent);
	}

	private GeoWaveData<SimpleFeature> postProcess(
			WayPoint point ) {
		switch (point.elementType) {
			case "trk": {
				Coordinate[] childSequence = point.buildCoordinates();
				trackBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createLineString(childSequence));
				boolean setDuration = true;
				if (point.minTime < Long.MAX_VALUE) {
					trackBuilder.set(
							"StartTimeStamp",
							new Date(
									point.minTime));
				}
				else {
					setDuration = false;

					trackBuilder.set(
							"StartTimeStamp",
							null);
				}
				if (point.maxTime > 0) {
					trackBuilder.set(
							"EndTimeStamp",
							new Date(
									point.maxTime));
				}
				else {
					setDuration = false;

					trackBuilder.set(
							"EndTimeStamp",
							null);
				}
				if (setDuration) {
					trackBuilder.set(
							"Duration",
							point.maxTime - point.minTime);
				}
				else {
					trackBuilder.set(
							"Duration",
							null);
				}

				trackBuilder.set(
						"NumberPoints",
						childSequence.length);
				trackBuilder.set(
						"TrackId",
						inputID.toString());
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
				 */
				return new GeoWaveData<SimpleFeature>(
						trackKey,
						primaryIndexId,
						trackBuilder.buildFeature(point.name + inputID));
			}
			case "rte": {
				if (point.build(waypointBuilder)) {
					Coordinate[] childSequence = point.buildCoordinates();
					trackBuilder.set(
							"geometry",
							GeometryUtils.GEOMETRY_FACTORY.createLineString(childSequence));
					boolean setDuration = true;
					if (point.minTime < Long.MAX_VALUE) {
						trackBuilder.set(
								"StartTimeStamp",
								new Date(
										point.minTime));
					}
					else {
						setDuration = false;

						trackBuilder.set(
								"StartTimeStamp",
								null);
					}
					if (point.maxTime > 0) {
						trackBuilder.set(
								"EndTimeStamp",
								new Date(
										point.maxTime));
					}
					else {
						setDuration = false;

						trackBuilder.set(
								"EndTimeStamp",
								null);
					}
					if (setDuration) {
						trackBuilder.set(
								"Duration",
								point.maxTime - point.minTime);
					}
					else {
						trackBuilder.set(
								"Duration",
								null);
					}

					trackBuilder.set(
							"NumberPoints",
							childSequence.length);
					trackBuilder.set(
							"TrackId",
							inputID.toString());
					/*
					 * trackBuilder.set( "UserId", gpxTrack.getUserid());
					 * trackBuilder.set( "User", gpxTrack.getUser());
					 * trackBuilder.set( "Description",
					 * gpxTrack.getDescription());
					 * 
					 * if ((gpxTrack.getTags() != null) &&
					 * (gpxTrack.getTags().size() > 0)) { final String tags =
					 * org.apache.commons.lang.StringUtils.join(
					 * gpxTrack.getTags(), TAG_SEPARATOR); trackBuilder.set(
					 * "Tags", tags); } else { trackBuilder.set( "Tags", null);
					 * }
					 */
					return new GeoWaveData<SimpleFeature>(
							trackKey,
							primaryIndexId,
							trackBuilder.buildFeature(point.name + inputID));

				}
				break;
			}
			case "metaData": {
				if (point.build(waypointBuilder)) {
					return new GeoWaveData<SimpleFeature>(
							waypointKey,
							primaryIndexId,
							waypointBuilder.buildFeature(point.name + inputID + "_" + point.lat.hashCode() + "_" + point.lon.hashCode()));

				}
				break;
			}
			case "wpt": {
				if (point.build(waypointBuilder)) {
					return new GeoWaveData<SimpleFeature>(
							waypointKey,
							primaryIndexId,
							waypointBuilder.buildFeature(point.name + inputID + "_" + point.lat.hashCode() + "_" + point.lon.hashCode()));

				}
				break;
			}
			case "rtept": {
				if (point.build(routeBuilder)) {
					return new GeoWaveData<SimpleFeature>(
							waypointKey,
							primaryIndexId,
							routeBuilder.buildFeature(point.name + inputID + "_" + point.lat.hashCode() + "_" + point.lon.hashCode()));

				}
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
							pointBuilder.buildFeature(inputID.toString() + "_" + (point.name != null ? point.name + "_" : "") + trackID++));
				}

				break;
			}
		}
		return null;
	}

	private boolean processElementAttributes(
			XMLEvent event,
			WayPoint point ) {
		StartElement node = event.asStartElement();
		final Iterator<Attribute> attributes = node.getAttributes();
		while (attributes.hasNext()) {
			final Attribute a = attributes.next();
			if (a.getName().getLocalPart().equals(
					"lon")) {
				point.lon = Double.parseDouble(a.getValue());
			}
			else {
				point.lat = Double.parseDouble(a.getValue());
			}
		}
		switch (node.getName().getLocalPart()) {
			case "ele": {
				point.elevation = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "magvar": {
				point.magvar = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "geoidheight": {
				point.geoidheight = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "name": {
				point.name = event.asCharacters().getData();
				break;
			}
			case "cmt": {
				point.cmt = event.asCharacters().getData();
				break;
			}
			case "desc": {
				point.desc = event.asCharacters().getData();
				break;
			}
			case "src": {
				point.src = event.asCharacters().getData();
				break;
			}
			case "text": {
				point.link = event.asCharacters().getData();
				break;
			}
			case "sym": {
				point.sym = event.asCharacters().getData();
				break;
			}
			case "type": {
				point.type = event.asCharacters().getData();
				break;
			}
			case "sat": {
				point.sat = Long.parseLong(event.asCharacters().getData());
				break;
			}
			case "vdop": {
				point.vdop = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "hdop": {
				point.hdop = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "pdop": {
				point.pdop = Double.parseDouble(event.asCharacters().getData());
				break;
			}
			case "time": {
				try {
					point.timestamp = GpxUtils.TIME_FORMAT_SECONDS.parse(
							event.asCharacters().getData()).getTime();

				}
				catch (final Exception t) {
					try {
						point.timestamp = GpxUtils.TIME_FORMAT_MILLIS.parse(
								event.asCharacters().getData()).getTime();
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
		long minTime = Long.MAX_VALUE;
		long maxTime = 0;

		public WayPoint(
				String myElType ) {
			this.elementType = myElType;
		}

		public void addChild(
				WayPoint child ) {
			if (children == null) {
				children = new ArrayList<WayPoint>();
			}
			children.add(child);
			updateTime(child.timestamp);

		}

		public Coordinate getCoordinate() {
			if (coordinate != null) return coordinate;
			if (lat != null && lon != null) coordinate = new Coordinate(
					lon,
					lat);
			return coordinate;
		}

		public Coordinate[] buildCoordinates() {
			if (children == null) return new Coordinate[0];
			Coordinate[] coords = new Coordinate[children.size()];
			for (int i = 0; i < coords.length; i++) {
				coords[i] = children.get(
						i).getCoordinate();
			}
			return coords;
		}

		private void updateTime(
				Long timestamp ) {
			if (timestamp != null) {
				minTime = Math.min(
						timestamp.longValue(),
						minTime);
				maxTime = Math.max(
						timestamp.longValue(),
						maxTime);
			}
		}

		public boolean build(
				SimpleFeatureBuilder waypointBuilder ) {
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

				return true;
			}
			else
				return false;
		}
	}
}