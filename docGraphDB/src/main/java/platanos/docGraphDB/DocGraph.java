package platanos.docGraphDB;

import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class DocGraph {
	protected static Cluster cluster;
	protected static Keyspace ksp;
	protected static DocVertices docVertices;
	static ColumnFamilyTemplate<byte[], byte[]> DocTemplate;
	protected static Docs docs;
	protected static Summaries summaries;

	public static void main(String[] args) {

		String[] arguments = Config.readLocalConfig();
		cluster = ClusterConnect.clusterConnect(arguments[0], arguments[1]);

		init();

	}

	static void init() {

		ksp = HFactory.createKeyspace("DocGraphUID", cluster);

		docVertices = new DocVertices(ksp);
		docs = new Docs(ksp);
		summaries = new Summaries(ksp);

	}
}
