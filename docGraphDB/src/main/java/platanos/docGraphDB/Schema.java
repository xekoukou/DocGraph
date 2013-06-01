package platanos.docGraphDB;

import java.util.Arrays;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.beans.DynamicComposite;
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
		

		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("DocGraphUID");

		if (keyspaceDef == null) {
			ColumnFamilyDefinition docVerticesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "DocVertices",
							ComparatorType.DYNAMICCOMPOSITETYPE);
			docVerticesDef.setComparatorTypeAlias(DynamicComposite.DEFAULT_DYNAMIC_COMPOSITE_ALIASES);
			

			ColumnFamilyDefinition docsDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Docs",
							ComparatorType.BYTESTYPE);

			ColumnFamilyDefinition summariesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Summaries",
							ComparatorType.BYTESTYPE);

			ColumnFamilyDefinition edgesDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "Edges",
							ComparatorType.COMPOSITETYPE);
			edgesDef.setComparatorTypeAlias("(UTF8Type , BytesType)");
			
			ColumnFamilyDefinition docMetaDataDef = HFactory
					.createColumnFamilyDefinition("DocGraphUID", "DocMetadata",
							ComparatorType.COMPOSITETYPE);
			docMetaDataDef.setComparatorTypeAlias("(UTF8Type , BytesType)");
			
			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"DocGraphUID", ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor, Arrays.asList(docVerticesDef, docsDef,
							summariesDef, edgesDef, docMetaDataDef));
 
			cluster.addKeyspace(newKeyspace, false);
		}
		keyspaceDef = cluster.describeKeyspace("LuceneUIDRanges");

		if (keyspaceDef == null) {

			ColumnFamilyDefinition luceneUIDs = HFactory
					.createColumnFamilyDefinition("LuceneUIDRanges",
							"LuceneUIDs", ComparatorType.LONGTYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"LuceneUIDRanges", ThriftKsDef.DEF_STRATEGY_CLASS,
					replicationFactor, Arrays.asList(luceneUIDs));

			cluster.addKeyspace(newKeyspace, false);

		}
		keyspaceDef = cluster.describeKeyspace("Sha3");

		if (keyspaceDef == null) {

			ColumnFamilyDefinition graphUID = HFactory
					.createColumnFamilyDefinition("Sha3", "GraphUIDs",
							ComparatorType.LONGTYPE);

			KeyspaceDefinition newKeyspace = HFactory.createKeyspaceDefinition(
					"Sha3", ThriftKsDef.DEF_STRATEGY_CLASS, replicationFactor,
					Arrays.asList(graphUID));

			cluster.addKeyspace(newKeyspace, false);

		}
	}

}
