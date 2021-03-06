package mil.nga.giat.geowave.store.index;

import mil.nga.giat.geowave.index.Persistable;
import mil.nga.giat.geowave.store.data.DataReader;
import mil.nga.giat.geowave.store.data.DataWriter;
import mil.nga.giat.geowave.store.dimension.DimensionField;

/**
 * This interface describes the common fields for all of the data within the
 * index. It is up to data adapters to map (encode) the native fields to these
 * common fields for persistence.
 */
public interface CommonIndexModel extends
		DataReader<CommonIndexValue>,
		DataWriter<Object, CommonIndexValue>,
		Persistable
{
	public DimensionField<? extends CommonIndexValue>[] getDimensions();

	public String getId();
}
