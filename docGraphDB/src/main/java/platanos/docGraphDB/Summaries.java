package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;

class Summaries {

	protected ColumnFamilyTemplate<Long, byte[]> template;

	Summaries(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<Long, byte[]>(ksp,
				"Summaries", LongSerializer.get(), BytesArraySerializer.get());
	}

	protected boolean addSummary(Long key, byte[] position, byte[] data) {
		try {

			ColumnFamilyUpdater<Long, byte[]> updater = template
					.createUpdater(key);

			updater.setByteArray(position, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected byte[] getSummary(Long key, byte[] position) {
		HColumn<byte[], byte[]> c = template.querySingleColumn(key, position,
				BytesArraySerializer.get());
		return c.getValue();
	}

}
