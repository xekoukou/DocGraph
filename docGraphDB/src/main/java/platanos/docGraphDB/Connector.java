package platanos.docGraphDB;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Poller;

import platanos.docGraphDB.Protocol.GraphView;
import platanos.docGraphDB.Protocol.GraphView.Builder;
import platanos.docGraphDB.Protocol.LoadCommand;
import platanos.docGraphDB.Protocol.MultiCommand;
import platanos.docGraphDB.Protocol.MultiCommand.Save;
import platanos.docGraphDB.Protocol.MultiSaveCommand;
import platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand;
import platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand.Type;
import platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand.Vertex.SecCommand;
import platanos.docGraphDB.Protocol.SearchView;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

class Connector {
	protected ZContext ctx;
	protected Socket router;
	protected Poller poller;
	protected PollItem pollitem;

	// actions
	protected DocVertices docVertices;
	protected Edges edges;
	protected Docs docs;
	protected Summaries summaries;
	protected DocMetadata docMetadata;

	Connector(DocVertices docVertices, Edges edges, Docs docs,
			Summaries summaries, DocMetadata docMetadata) {
		this.docVertices = docVertices;
		this.edges = edges;
		this.docs = docs;
		this.summaries = summaries;
		this.docMetadata = docMetadata;

		ctx = new ZContext();
		router = ctx.createSocket(ZMQ.ROUTER);

		int ok = router.bind("tcp://127.0.0.1:49151");
		assert (ok != -1);
		pollitem = new PollItem(router, ZMQ.Poller.POLLIN);
		poller = new Poller(1);
		poller.register(pollitem);

	}

	private class Pair {
		public boolean success;
		public byte[] data;
	}

	void poll() {
		while (true) {
			int ok = poller.poll();
			assert (ok != -1);

			if (poller.pollin(0)) {
				ZMsg msg = ZMsg.recvMsg(router);

				ZFrame address = msg.unwrap();
				ZFrame request = msg.pop();

				byte[] data = request.getData();
				try {
					MultiCommand multicommand = MultiCommand.parseFrom(data);
					if (Save.TRUE == multicommand.getSave()) {
						if (handleSaveCommand(multicommand
								.getMultiSaveCommand())) { // in case of
							// failure we do
							// nothing
							msg.wrap(address);
							msg.send(router);
						}
					} else {
						Pair pair = handleLoadCommand(multicommand
								.getLoadCommand());
						if (pair.success) {
							msg.add(pair.data);
							msg.wrap(address);
							msg.send(router);
						}
					}
				} catch (InvalidProtocolBufferException e) {
					address.destroy();

				}
				request.destroy();
				msg.destroy();
			}
		}
	}

	boolean handleSaveCommand(MultiSaveCommand multicommand) {
		boolean result = true;
		Long key = multicommand.getKey();
		for (int i = 0; i < multicommand.getSaveCommandCount(); i++) {
			SaveCommand command = multicommand.getSaveCommand(i);
			Type type = command.getType();

			SecCommand sec;
			platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand.Edge.SecCommand sec2;
			platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand.Content.SecCommand sec3;
			platanos.docGraphDB.Protocol.MultiSaveCommand.SaveCommand.Metadata.SecCommand sec4;
			switch (type) {
			case cvertex:
				sec = command.getVertex().getSecCommand();
				switch (sec) {
				case ccreateVertex:
					result = docVertices.addVertex(key);
					break;
				case csetSize:
					result = docVertices.setSize(key, command.getVertex()
							.getSize().toByteArray());
					break;
				case caddEdge:
					result = docVertices.addEdge(key, command.getVertex()
							.getEdge().toByteArray());
					break;
				case caddBack_edge:
					result = docVertices.addBackEdge(key, command.getVertex()
							.getEdge().toByteArray());
					break;
				}
				break;
			case cedge:
				sec2 = command.getEdge().getSecCommand();
				switch (sec2) {
				case cinEdge:
					result = edges.addInEdge(key, command.getEdge()
							.getPosition().toByteArray(), command.getEdge()
							.getData().toByteArray());
					break;
				case cedge:
					result = edges.addEdge(key, command.getEdge().getEdge()
							.toByteArray(), command.getEdge().getData()
							.toByteArray());
					break;

				}
				break;
			case ccontent:
				sec3 = command.getContent().getSecCommand();
				switch (sec3) {
				case cdoc:
					result = docs.addDoc(key, command.getContent()
							.getPosition().toByteArray(), command.getContent()
							.getData().toByteArray());
					break;
				case csummary:
					result = summaries.addSummary(key, command.getContent()
							.getPosition().toByteArray(), command.getContent()
							.getData().toByteArray());
					break;
				}
				break;
			case cmetadata:
				sec4 = command.getMetadata().getSecCommand();
				switch (sec4) {
				case csha3:
					result = docMetadata.addSha3(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getSha3().toByteArray());
					break;
				case cluceneUid:
					result = docMetadata.addLuceneUID(key, command
							.getMetadata().getPosition().toByteArray(), command
							.getMetadata().getLuceneUid());
					break;
				case ctime:
					result = docMetadata.addTime(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getTime());
					break;
				case cauthor:
					result = docMetadata.addAuthor(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getAuthor().toByteArray());
					break;
				case clocation:
					result = docMetadata.addLocation(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getLocation().toByteArray());
					break;
				case cseller:
					result = docMetadata.addSeller(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getSeller().toByteArray());
					break;
				case cperiod:
					result = docMetadata.addPeriod(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getPeriod().toByteArray());
					break;
				case csha3Contract:
					result = docMetadata.addSha3Contract(key, command
							.getMetadata().getPosition().toByteArray(), command
							.getMetadata().getSha3Contract().toByteArray());
					break;
				case cprevSha3:
					result = docMetadata.addPrevSha3(key, command.getMetadata()
							.getPosition().toByteArray(), command.getMetadata()
							.getPrevSha3List());
					break;
				}
				break;
			}
			if (result == false) {
				return result;
			}
		}
		return result;
	}

	// TODO exceptions in Vertex?
	Pair handleLoadCommand(LoadCommand loadCommand) {
		Pair result = new Pair();
		result.success = true;

		platanos.docGraphDB.Protocol.LoadCommand.Type type = loadCommand
				.getType();

		switch (type) {
		case cgraphView:
			Builder gv = GraphView.newBuilder();
			for (int i = 0; i <= loadCommand.getGraphView().getKeyCount(); i++) {
				long key = loadCommand.getGraphView().getKey(i);
				byte[] position = loadCommand.getGraphView().getPosition(i)
						.toByteArray();
				gv.setSummaries(i, ByteString.copyFrom(summaries.getSummary(
						key, position)));
				gv.setSha3(i,
						ByteString.copyFrom(docMetadata.getSha3(key, position)));
			}

			for (int i = 0; i <= loadCommand.getGraphView().getEdgesCount(); i++) {
				gv.setEdges(i, ByteString.copyFrom(edges.getEdge(loadCommand
						.getGraphView().getEdges(i))));
			}
			result.data = gv.build().toByteArray();
			return result;
		case ccontentView:
			result.data = docs.getDoc(loadCommand.getContentView().getKey(),
					loadCommand.getContentView().getStartPosition()
							.toByteArray(), loadCommand.getContentView()
							.getPosition().toByteArray());
			return result;
		case csearchView:
			platanos.docGraphDB.Protocol.SearchView.Builder sv = SearchView
					.newBuilder();
			for (int i = 0; i <= loadCommand.getSearchView().getKeyCount(); i++) {
				long key = loadCommand.getSearchView().getKey(i);
				byte[] position = loadCommand.getSearchView().getPosition(i)
						.toByteArray();
				sv.setSummaries(i, ByteString.copyFrom(summaries.getSummary(
						key, position)));
				sv.setSha3(i,
						ByteString.copyFrom(docMetadata.getSha3(key, position)));
			}

			result.data = sv.build().toByteArray();
			return result;
		case cvertex:
			result.data = docVertices.getVertex(loadCommand.getVertex()
					.getKey());
			return result;

			// TODO add repair method
		}
		result.success = false;
		return result;
	}

}
