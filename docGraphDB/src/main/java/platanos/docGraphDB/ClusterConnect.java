package platanos.docGraphDB;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

class ClusterConnect {

	static Cluster clusterConnect(String name, String location) {
		return HFactory.getOrCreateCluster(name, location);
	}
}
