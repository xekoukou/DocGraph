package platanos.docGraphDB;

import platanos.docGraphDB.Protocol.ContentView;
import platanos.docGraphDB.Protocol.ContentView.Builder;

import com.google.protobuf.ByteString;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

class Docs {

	protected ColumnFamilyTemplate<Long, byte[]> template;
	protected Keyspace ksp;

	Docs(Keyspace ksp) {
		template = new ThriftColumnFamilyTemplate<Long, byte[]>(ksp, "Docs",
				LongSerializer.get(), BytesArraySerializer.get());
		this.ksp = ksp;
	}

	protected boolean addDoc(Long key, byte[] position, byte[] data) {
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

	protected byte[] getDoc(Long key, byte[] start_position, byte[] position) {
		Builder cv = ContentView.newBuilder();

		SliceQuery<Long, byte[], byte[]> sliceQuery = HFactory
				.createSliceQuery(ksp, LongSerializer.get(),
						BytesArraySerializer.get(), BytesArraySerializer.get());
		sliceQuery.setColumnFamily("Docs");
		sliceQuery.setKey(key);

		ColumnSliceIterator<Long, byte[], byte[]> sliceIterator = new ColumnSliceIterator<Long, byte[], byte[]>(
				sliceQuery, start_position, position, false);

		while (sliceIterator.hasNext()) {
			HColumn<byte[], byte[]> sres = sliceIterator.next();
			byte[] data = sres.getValue();
			cv.addData(ByteString.copyFrom(data));
		}
		return cv.build().toByteArray();
	}

}
