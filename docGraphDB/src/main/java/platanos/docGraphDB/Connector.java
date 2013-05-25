package platanos.docGraphDB;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.PollItem;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQ.Poller;

import com.google.protobuf.InvalidProtocolBufferException;

import platanos.docGraphDB.Protocol.MultiCommand;
import platanos.docGraphDB.Protocol.MultiCommand.LoadCommand;
import platanos.docGraphDB.Protocol.MultiCommand.SaveCommand;


class Connector {
	protected ZContext ctx;
	protected Socket router;
	protected Poller poller;
	protected PollItem pollitem;
	
	Connector(){
	ctx = new ZContext();
	router = ctx.createSocket(ZMQ.ROUTER);
	
	int ok = router.bind("tcp://127.0.0.1:49151");
	assert( ok != -1);
    	pollitem = new PollItem(router,ZMQ.Poller.POLLIN);
	poller = new Poller(1);
	poller.register(pollitem);
	
	}
	
	void poll(){
		while(true){
		int	ok = poller.poll();
			assert(ok != -1);
			
			if(poller.pollin(0)){
				ZMsg msg = ZMsg.recvMsg(router);
				
				ZFrame address = msg.unwrap();
				ZFrame request = msg.pop();
				
				byte[] data = request.getData();
				try{
				MultiCommand multicommand = MultiCommand.parseFrom(data);
				data = handle(multicommand);
				msg.add(data);
				msg.wrap(address);
				msg.send(router);
				
				}catch(InvalidProtocolBufferException e){
					address.destroy();

				}	
				request.destroy();
				msg.destroy();
			}
		}
	}
	
	byte[] handle(MultiCommand multicommand){
		long key = multicommand.getKey();
		for(int i = 0; i< multicommand.getSaveCommandCount(); i++){
			 SaveCommand command = multicommand.getSaveCommand(i);
			
		}
		for(int i = 0; i< multicommand.getLoadCommandCount(); i++){
			 LoadCommand command = multicommand.getLoadCommand(i);
			
		}
	}
	
	

}
