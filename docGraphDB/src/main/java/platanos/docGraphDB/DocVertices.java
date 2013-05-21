package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.exceptions.HectorException;

class DocVertices {

	protected ColumnFamilyTemplate<byte[], Composite> template;

	DocVertices(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<byte[], Composite>(ksp,
				"DocVertices", BytesArraySerializer.get(),
				new CompositeSerializer());
	}

	protected boolean addEdge(byte[] key, byte[] data) {
		try {

			Composite columnName = new Composite();

			columnName.add(0, "edge");
			columnName.add(1, data);

			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, new byte[0]);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected boolean addBackEdge(byte[] key, byte[] data) {
		try {

			Composite columnName = new Composite();

			columnName.add(0, "back_edge");
			columnName.add(1, data);

			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);

			updater.setByteArray(columnName, new byte[0]);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected boolean addVertex(byte[] key) {
		try {

			Composite columnName = new Composite();
			columnName.add(0, "size");
			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);
			byte[] zero = new byte[1];
			zero[0] = 0;
			updater.setByteArray(columnName, zero);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}

	}
	//this is in fact an increment
	protected boolean setSize(byte[] key, byte[] size) {
		try {

			Composite columnName = new Composite();
			columnName.add(0, "size");
			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, size);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}

	}

}
