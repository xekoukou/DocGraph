package platanos.docGraphDB;

import com.google.protobuf.ByteString;

import platanos.docGraphDB.Protocol.Vertex;
import platanos.docGraphDB.Protocol.Vertex.Builder;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

class DocVertices {

	protected ColumnFamilyTemplate<Long, DynamicComposite> template;
	protected Keyspace ksp;

	DocVertices(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<Long, DynamicComposite>(ksp,
				"DocVertices", LongSerializer.get(),
				new DynamicCompositeSerializer());
		this.ksp = ksp;
	}

	protected boolean addEdge(Long key, byte[] data) {
		try {

			DynamicComposite columnName = new DynamicComposite();

			columnName.add(0, "edge");
			columnName.add(1, data);

			ColumnFamilyUpdater<Long, DynamicComposite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, new byte[0]);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected boolean addBackEdge(Long key, byte[] data) {
		try {

			DynamicComposite columnName = new DynamicComposite();

			columnName.add(0, "back_edge");
			columnName.add(1, data);

			ColumnFamilyUpdater<Long, DynamicComposite> updater = template
					.createUpdater(key);

			updater.setByteArray(columnName, new byte[0]);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}
	}

	protected boolean addVertex(Long key) {
		try {

			DynamicComposite columnName = new DynamicComposite();
			columnName.add(0, "size");
			ColumnFamilyUpdater<Long, DynamicComposite> updater = template
					.createUpdater(key);
			byte[] zero = new byte[0];
			updater.setByteArray(columnName, zero);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}

	}

	// this is in fact an increment
	protected boolean setSize(Long key, byte[] size) {
		try {

			DynamicComposite columnName = new DynamicComposite();
			columnName.add(0, "size");
			ColumnFamilyUpdater<Long, DynamicComposite> updater = template
					.createUpdater(key);
			updater.setByteArray(columnName, size);
			template.update(updater);

			return true;
		} catch (HectorException e) {
			return false;
		}

	}

	protected byte[] getVertex(Long key) {

		Builder vb = Vertex.newBuilder();

		// size
		DynamicComposite comp = new DynamicComposite();
		comp.add(0, "size");

		HColumn<DynamicComposite, byte[]> res = template.querySingleColumn(key,
				comp, BytesArraySerializer.get());
		vb.setSize(ByteString.copyFrom(res.getValue()));

		// edge
		DynamicComposite start = new DynamicComposite();
		start.addComponent(0, "edge", Composite.ComponentEquality.EQUAL);
		DynamicComposite end = new DynamicComposite();
		end.addComponent(0, "edge",
				Composite.ComponentEquality.GREATER_THAN_EQUAL);

		SliceQuery<Long, DynamicComposite, byte[]> sliceQuery = HFactory
				.createSliceQuery(ksp, LongSerializer.get(),
						new DynamicCompositeSerializer(),
						BytesArraySerializer.get());
		sliceQuery.setColumnFamily("CountryStateCity");
		sliceQuery.setKey(key);

		ColumnSliceIterator<Long, DynamicComposite, byte[]> sliceIterator = new ColumnSliceIterator<Long, DynamicComposite, byte[]>(
				sliceQuery, start, end, false);
		HColumn<DynamicComposite, byte[]> sres;
		int i = 0;
		while (sliceIterator.hasNext()) {
			sres = sliceIterator.next();
			byte[] data = sres.getName().get(1, BytesArraySerializer.get());
			vb.setEdges(i, (ByteString.copyFrom(data)));
			i++;
		}

		// back_edges
		start = new DynamicComposite();
		start.addComponent(0, "edge", Composite.ComponentEquality.EQUAL);
		end = new DynamicComposite();
		end.addComponent(0, "edge",
				Composite.ComponentEquality.GREATER_THAN_EQUAL);

		sliceQuery = HFactory.createSliceQuery(ksp, LongSerializer.get(),
				new DynamicCompositeSerializer(), BytesArraySerializer.get());
		sliceQuery.setColumnFamily("CountryStateCity");
		sliceQuery.setKey(key);

		sliceIterator = new ColumnSliceIterator<Long, DynamicComposite, byte[]>(
				sliceQuery, start, end, false);
		i = 0;
		while (sliceIterator.hasNext()) {
			sres = sliceIterator.next();
			byte[] data = sres.getName().get(1, BytesArraySerializer.get());
			vb.setEdges(i, (ByteString.copyFrom(data)));
			i++;
		}

		return vb.build().toByteArray();

	}

}
