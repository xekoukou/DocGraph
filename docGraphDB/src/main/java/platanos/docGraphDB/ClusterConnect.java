package platanos.docGraphDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

class ClusterConnect {

	static Cluster clusterConnect(String name, String location) {
		Logger log = LoggerFactory.getLogger(ClusterConnect.class);
		log.info(location);

		return HFactory.getOrCreateCluster(name, location);
	}
}
