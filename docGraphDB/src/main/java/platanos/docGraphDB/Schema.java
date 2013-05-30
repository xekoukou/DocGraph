package platanos.docGraphDB;

import java.util.Arrays;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

public class Schema {

	public static void main(String[] args) {
		if (args.length == 3) {
			Cluster cluster = ClusterConnect.clusterConnect(args[0], args[1]);
			try {
				int replicationFactor = Integer.parseInt(args[2]);
				createSchema(cluster, replicationFactor);
			} catch (NumberFormatException e) {
				System.out
						.print("Wrong arguments: \n command name location replicationFactor");
			}
		} else {
			System.out
					.print("Wrong arguments: \n command name location replicationFactor");
		}

	}

	static void createSchema(Cluster cluster, int replicationFactor) {

		KeyspaceDefinition keyspaceDef = cluster
				.describeKeyspace("DocGraphUID");

		if (keyspaceDef == null) {
			ColumnFamilyDefinition DocVerticesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "DocVertices",
							ComparatorType.DYNAMICCOMPOSITETYPE);

			ColumnFamilyDefinition DocsDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Docs",
							ComparatorType.BYTESTYPE);

			ColumnFamilyDefinition SummariesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Summaries",
							ComparatorType.BYTESTYPE);

			ColumnFamilyDefinition EdgesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Edges",
							ComparatorType.COMPOSITETYPE);

			ColumnFamilyDefinition DocMetaDataDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "DocMetadata",
							ComparatorType.COMPOSITETYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"DOcGraphUID", ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor, Arrays.asList(DocVerticesDef, DocsDef,
							SummariesDef, EdgesDef, DocMetaDataDef));

			cluster.addKeyspace(newKeyspace, true);
		}
		keyspaceDef = cluster
				.describeKeyspace("LuceneUIDRanges");

		if (keyspaceDef == null) {
			
			ColumnFamilyDefinition luceneUIDs = HFactory
					.createColumnFamilyDefinition("LuceneUIDRanges", "LuceneUIDs",
							ComparatorType.LONGTYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"LuceneUIDRanges", ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor, Arrays.asList(luceneUIDs));

			cluster.addKeyspace(newKeyspace, true);
			
		}
		keyspaceDef = cluster
				.describeKeyspace("Sha3");

		if (keyspaceDef == null) {
			
			ColumnFamilyDefinition graphUID = HFactory
					.createColumnFamilyDefinition("Sha3", "GraphUIDs",
							ComparatorType.LONGTYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"Sha3", ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor, Arrays.asList(graphUID));

			cluster.addKeyspace(newKeyspace, true);
			
		}
	}

}
