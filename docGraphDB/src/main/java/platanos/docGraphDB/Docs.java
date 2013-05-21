package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;

class Docs {

	protected ColumnFamilyTemplate<byte[], byte[]> template;

	Docs(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<byte[], byte[]>(ksp, "Docs",
				BytesArraySerializer.get(), BytesArraySerializer.get());
	}

	protected boolean addDoc(byte[] key, byte[] position, byte[] data) {
		try {

			ColumnFamilyUpdater<byte[], byte[]> updater = template
					.createUpdater(key);

			updater.setByteArray(position, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

}
