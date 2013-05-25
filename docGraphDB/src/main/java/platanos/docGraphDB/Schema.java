package platanos.docGraphDB;


import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.BytesArraySerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;


public class Schema {

	public static void main(String[] args) {
		if (args.length == 3) {
			 Keyspace ksp = ClusterConnect.clusterConnect(args[0], args[1]);
			try {
				int replicationFactor = Integer.parseInt(args[2]);
				createSchema(ksp, replicationFactor);
			} catch (NumberFormatException e) {
				System.out
						.print("Wrong arguments: \n command name location replicationFactor");
			}
		} else {
			System.out
					.print("Wrong arguments: \n command name location replicationFactor");
		}

	}

	static void createSchema(Keyspace ksp, int replicationFactor) {
		
		try {
			ksp.createKeyspace(ImmutableMap.<String, Object>builder()
				    .put("strategy_options", ImmutableMap.<String, Object>builder()
				        .put("replication_factor", new Integer(replicationFactor).toString())
				        .build())
				    .put("strategy_class",     "SimpleStrategy")
				        .build()
				     );

		ColumnFamily<byte[], Composite> DocVertices = ColumnFamily
				.newColumnFamily("DocVertices", BytesArraySerializer.get(), new CompositeSerializer());
			ksp.createColumnFamily(DocVertices, null);

		
		ColumnFamily<byte[], Composite> Edges = ColumnFamily
				.newColumnFamily("Edges", BytesArraySerializer.get(), new CompositeSerializer());
		ksp.createColumnFamily(Edges, null);
		
		ColumnFamily<byte[], Composite> DocMetadata = ColumnFamily
				.newColumnFamily("DocMetadata", BytesArraySerializer.get(), new CompositeSerializer());
		ksp.createColumnFamily(DocMetadata, null);
		
		ColumnFamily<byte[], byte[]> Docs = ColumnFamily
				.newColumnFamily("Docs", BytesArraySerializer.get(), BytesArraySerializer.get());
		ksp.createColumnFamily(Docs, null);
		
		ColumnFamily<byte[], byte[]> Summaries = ColumnFamily
				.newColumnFamily("Summaries", BytesArraySerializer.get(), BytesArraySerializer.get());
		ksp.createColumnFamily(Summaries, null);
		
		} catch (ConnectionException e) {
			System.out.print("Error trying to create the Schema - Connection Error");
			e.printStackTrace();
		}
		
		

		}

}


