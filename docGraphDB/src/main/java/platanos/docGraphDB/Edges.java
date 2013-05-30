package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.exceptions.HectorException;

class Edges {

	protected ColumnFamilyTemplate<Long, Composite> template;

	Edges(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<Long, Composite>(ksp,
				"Docs", LongSerializer.get(), new CompositeSerializer());
	}

	// edge here is the edge struct used in DocVertices as well
	// data is the commit summary
	protected boolean addEdge(Long key, byte[] edge, byte[] data) {
		try {
			Composite columnName = new Composite();
			columnName.add(0, "edges");
			columnName.add(1, edge);

			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	// position here is the position of the initial Doc inside the common Vertex
	protected boolean addInEdge(Long key, byte[] position, byte[] data) {
		try {
			Composite columnName = new Composite();
			columnName.add(0, "inEdges");
			columnName.add(1, position);

			ColumnFamilyUpdater<Long, Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

}
