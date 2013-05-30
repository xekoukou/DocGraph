package platanos.docGraphDB;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;

public class DocGraph {
	protected static Cluster cluster;
	protected static Keyspace ksp;
	protected static Keyspace rksp;
	protected static Keyspace sksp;

	public static void main(String[] args) {

		String[] arguments = Config.readLocalConfig();
		cluster = ClusterConnect.clusterConnect(arguments[0], arguments[1]);

		init();

	}

	static void init() {

		ksp = HFactory.createKeyspace("DocGraphUID", cluster);
		rksp = HFactory.createKeyspace("LuceneUIDRanges", cluster);
		sksp = HFactory.createKeyspace("Sha1", cluster);

		DocVertices docVertices = new DocVertices(ksp);
		Docs docs = new Docs(ksp);
		Summaries summaries = new Summaries(ksp);
		Edges edges = new Edges(ksp);
		DocMetadata docMetadata = new DocMetadata(ksp,rksp,sksp);

		Connector connector = new Connector(docVertices, edges, docs,
				summaries, docMetadata);
		connector.poll();

	}
}
