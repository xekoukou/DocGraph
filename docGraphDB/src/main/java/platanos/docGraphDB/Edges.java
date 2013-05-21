package platanos.docGraphDB;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.exceptions.HectorException;

class Edges {

	protected ColumnFamilyTemplate<byte[], Composite> template;

	Edges(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<byte[], Composite>(ksp, "Docs",
				BytesArraySerializer.get(), new CompositeSerializer());
	}
//edge here is the edge struct used in DocVertices as well
// data is the commit summary
	protected boolean addEdge(byte[] key, byte[] edge, byte[] data) {
		try {
Composite columnName= new Composite();
columnName.add(0, "edges");
columnName.add(1,edge);
			
			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}
	//position here is the position of the initial Doc inside the common Vertex
	protected boolean addInEdge(byte[] key, byte[] position, byte[] data) {
		try {
Composite columnName= new Composite();
columnName.add(0, "inEdges");
columnName.add(1,position);
			
			ColumnFamilyUpdater<byte[], Composite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, data);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

}
