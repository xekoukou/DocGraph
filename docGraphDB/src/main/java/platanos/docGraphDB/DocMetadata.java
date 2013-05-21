package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HectorException;

class DocMetadata {

	protected ColumnFamilyTemplate<byte[], String> template;

	DocMetadata(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<byte[], String>(ksp,
				"DocMetadata", BytesArraySerializer.get(),
				StringSerializer.get());
	}

	protected boolean addSha1(byte[] key, byte[] data) {
		try {

			ColumnFamilyUpdater<byte[], String> updater = template
					.createUpdater(key);

			updater.setByteArray("sha1", data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}
	
	protected boolean addLuceneUID(byte[] key, byte[] data) {
		try {

			ColumnFamilyUpdater<byte[], String> updater = template
					.createUpdater(key);

			updater.setByteArray("luceneUID", data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

}
