package mil.nga.giat.geowave.types.gpx;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.TimeUnit;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.ingest.GeoWaveData;
import mil.nga.giat.geowave.ingest.hdfs.StageToHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestFromHdfsPlugin;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithMapper;
import mil.nga.giat.geowave.ingest.hdfs.mapreduce.IngestWithReducer;
import mil.nga.giat.geowave.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.xml.sax.SAXException;

import com.google.common.collect.Iterators;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * This plugin is used for ingesting any GPX formatted data from a local file
 * system into GeoWave as GeoTools' SimpleFeatures. It supports the default
 * configuration of spatial and spatial-temporal indices and it will support
 * wither directly ingesting GPX data from a local file system to GeoWave or to
 * stage the data in an intermediate format in HDFS and then to ingest it into
 * GeoWave using a map-reduce job. It supports OSM metadata.xml files if the
 * file is directly in the root base directory that is passed in command-line to
 * the ingest framework.
 */
public class GpxIngestPlugin implements
		LocalFileIngestPlugin<SimpleFeature>,
		IngestFromHdfsPlugin<GpxTrack, SimpleFeature>,
		StageToHdfsPlugin<GpxTrack>
{

	private final static Logger LOGGER = Logger.getLogger(GpxIngestPlugin.class);

	private final static String TAG_SEPARATOR = " ||| ";

	private Map<Long, GpxTrack> metadata;
	private static long currentFreeTrackId = 0;

	private final SimpleFeatureBuilder pointBuilder;
	private final SimpleFeatureBuilder waypointBuilder;
	private final SimpleFeatureBuilder routeBuilder;
	private final SimpleFeatureBuilder trackBuilder;
	private final SimpleFeatureType pointType;
	private final SimpleFeatureType waypointType;
	private final SimpleFeatureType trackType;

	private final ByteArrayId pointKey;
	private final ByteArrayId waypointKey;
	private final ByteArrayId trackKey;

	private final Index[] supportedIndices;

	public GpxIngestPlugin() {

		pointType = GpxUtils.createGPXPointDataType();
		waypointType = GpxUtils.createGPXWaypointDataType();
		trackType = GpxUtils.createGPXTrackDataType();

		pointKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_POINT_FEATURE));
		waypointKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_WAYPOINT_FEATURE));
		trackKey = new ByteArrayId(
				StringUtils.stringToBinary(GpxUtils.GPX_TRACK_FEATURE));
		pointBuilder = new SimpleFeatureBuilder(
				pointType);
		waypointBuilder = new SimpleFeatureBuilder(
				waypointType);
		trackBuilder = new SimpleFeatureBuilder(
				trackType);
		supportedIndices = new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex(),
			IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex()
		};

	}

	@Override
	public String[] getFileExtensionFilters() {
		return new String[] {
			"xml",
			"gpx"
		};
	}

	@Override
	public void init(
			final File baseDirectory ) {
		final File f = new File(
				baseDirectory,
				"metadata.xml");
		if (!f.exists()) {
			LOGGER.warn("No metadata file found - looked at: " + f.getAbsolutePath());
			LOGGER.warn("No metadata will be loaded");
		}
		else {
			try {
				long time = System.currentTimeMillis();
				metadata = GpxUtils.parseOsmMetadata(f);
				time = System.currentTimeMillis() - time;
				final String timespan = String.format(
						"%d min, %d sec",
						TimeUnit.MILLISECONDS.toMinutes(time),
						TimeUnit.MILLISECONDS.toSeconds(time) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(time)));
				LOGGER.info("Metadata parsed in in " + timespan + " for " + metadata.size() + " tracks");
			}
			catch (final XMLStreamException | FileNotFoundException e) {
				LOGGER.warn(
						"Unable to read OSM metadata file: " + f.getAbsolutePath(),
						e);
			}
		}

	}

	@Override
	public boolean supportsFile(
			final File file ) {
		// if its a gpx extension assume it is supported
		if (file.getName().toLowerCase().endsWith(
				"gpx")) {
			return true;
		}
		// otherwise take a quick peek at the file to ensure it matches the GPX
		// schema
		try {
			return GpxUtils.validateGpx(file);
		}
		catch (SAXException | IOException e) {
			LOGGER.warn(
					"Unable to read file:" + file.getAbsolutePath(),
					e);
		}
		return false;
	}

	@Override
	public Index[] getSupportedIndices() {
		return supportedIndices;
	}

	@Override
	public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
			final String globalVisibility ) {
		final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler = ((globalVisibility != null) && !globalVisibility.isEmpty()) ? new GlobalVisibilityHandler<SimpleFeature, Object>(
				globalVisibility) : null;
		return new WritableDataAdapter[] {
			new FeatureDataAdapter(
					pointType,
					fieldVisiblityHandler),
			new FeatureDataAdapter(
					waypointType,
					fieldVisiblityHandler),
			new FeatureDataAdapter(
					trackType,
					fieldVisiblityHandler)
		};
	}

	@Override
	public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
			final File input,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final GpxTrack[] gpxTracks = toHdfsObjects(input);
		final List<CloseableIterator<GeoWaveData<SimpleFeature>>> allData = new ArrayList<CloseableIterator<GeoWaveData<SimpleFeature>>>();
		for (final GpxTrack track : gpxTracks) {
			final CloseableIterator<GeoWaveData<SimpleFeature>> geowaveData = toGeoWaveDataInternal(
					track,
					primaryIndexId,
					globalVisibility);
			allData.add(geowaveData);
		}
		return new CloseableIterator.Wrapper<GeoWaveData<SimpleFeature>>(
				Iterators.concat(allData.iterator()));
	}

	@Override
	public Schema getAvroSchemaForHdfsType() {
		return GpxTrack.getClassSchema();
	}

	@Override
	public GpxTrack[] toHdfsObjects(
			final File input ) {
		GpxTrack track = null;
		if (metadata != null) {
			try {
				final long id = Long.parseLong(FilenameUtils.removeExtension(input.getName()));
				track = metadata.remove(id);
			}
			catch (final NumberFormatException e) {
				LOGGER.info(
						"OSM metadata found, but track file name is not a numeric ID",
						e);
			}
		}
		if (track == null) {
			track = new GpxTrack();
			track.setTrackid(currentFreeTrackId++);
		}

		try {
			track.setGpxfile(ByteBuffer.wrap(Files.readAllBytes(input.toPath())));
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to read GPX file: " + input.getAbsolutePath(),
					e);
		}

		return new GpxTrack[] {
			track
		};
	}

	@Override
	public boolean isUseReducerPreferred() {
		return false;
	}

	@Override
	public IngestWithMapper<GpxTrack, SimpleFeature> ingestWithMapper() {
		return new IngestGpxTrackFromHdfs(
				this);
	}

	@Override
	public IngestWithReducer<GpxTrack, ?, ?, SimpleFeature> ingestWithReducer() {
		// unsupported right now
		throw new UnsupportedOperationException(
				"GPX tracks cannot be ingested with a reducer");
	}

	private CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveDataInternal(
			final GpxTrack gpxTrack,
			final ByteArrayId primaryIndexId,
			final String globalVisibility ) {
		final InputStream in = new ByteArrayInputStream(
				gpxTrack.getGpxfile().array());
		return  new GPXConsumer(
				in,
				primaryIndexId,
				gpxTrack.getTrackid(),
				globalVisibility);
	}

	@Override
	public Index[] getRequiredIndices() {
		return new Index[] {};
	}

	public static class IngestGpxTrackFromHdfs implements
			IngestWithMapper<GpxTrack, SimpleFeature>
	{
		private final GpxIngestPlugin parentPlugin;

		public IngestGpxTrackFromHdfs() {
			this(
					new GpxIngestPlugin());
			// this constructor will be used when deserialized
		}

		public IngestGpxTrackFromHdfs(
				final GpxIngestPlugin parentPlugin ) {
			this.parentPlugin = parentPlugin;
		}

		@Override
		public WritableDataAdapter<SimpleFeature>[] getDataAdapters(
				final String globalVisibility ) {
			return parentPlugin.getDataAdapters(globalVisibility);
		}

		@Override
		public CloseableIterator<GeoWaveData<SimpleFeature>> toGeoWaveData(
				final GpxTrack input,
				final ByteArrayId primaryIndexId,
				final String globalVisibility ) {
			return parentPlugin.toGeoWaveDataInternal(
					input,
					primaryIndexId,
					globalVisibility);
		}

		@Override
		public byte[] toBinary() {
			return new byte[] {};
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}

	private class GPXConsumer implements
			CloseableIterator<GeoWaveData<SimpleFeature>>
	{

		final InputStream fileStream;
		final ByteArrayId primaryIndexId;
		final Long inputID;
		final String globalVisibility;
		long trackID = 0;

		final XMLInputFactory inputFactory = XMLInputFactory.newInstance();
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
						case "gpx": continue;
						default: return xx(eventReader, node.getName().getLocalPart());
					}
				}
			}
			return null;
		}

		Stack<WayPoint> currentPointStack = new Stack<WayPoint>();

		private GeoWaveData<SimpleFeature> xx(
				XMLEventReader eventReader,
				String elType )
				throws XMLStreamException {
			XMLEvent event = eventReader.peek();
			WayPoint point = currentPointStack.peek();

			while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
					elType))) {
				if (event.isStartElement()) {
					if (!dx(
							event,
							point)) {
						WayPoint child = new WayPoint();
						currentPointStack.push(child);
						GeoWaveData<SimpleFeature> newFeature = xx(
								eventReader,
								event.asStartElement().getName().getLocalPart());
						currentPointStack.pop();
						if (newFeature != null) {
							point.addChild(child);
							return newFeature;
						}
					}
				}
				eventReader.nextEvent();
				event = eventReader.peek();
			}
			return processEnd(
					elType,
					point);
		}

		private GeoWaveData<SimpleFeature> processEnd(
				String elType,
				WayPoint point ) {
			switch (elType) {
				case "trk": {
					Coordinate[] childSequence =point.buildCoordinates();	
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

					return new GeoWaveData<SimpleFeature>(
							trackKey,
							primaryIndexId,
							trackBuilder.buildFeature(gpxTrack.getTrackid().toString()));
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
		}

		private WayPoint parseWayPointTypeElement(
				XMLEventReader eventReader,
				String elType )
				throws XMLStreamException {
			XMLEvent event = eventReader.peek();
			WayPoint point = new WayPoint();

			while (!(event.isEndElement() && event.asEndElement().getName().getLocalPart().equals(
					elType))) {
				if (event.isStartElement()) {
					dx(
							event,
							point);
				}
				eventReader.nextEvent();
				event = eventReader.peek();
			}
			return point;
		}
	}

	private boolean dx(
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

		Coordinate coordinate = null;
		List<WayPoint> children = null;
		long minTime = Long.MAX_VALUE;
		long maxTime = 0;

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
			for (int i =0 ; i < coords.length; i++) {
				coords[i] = children.get(i).getCoordinate();
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
