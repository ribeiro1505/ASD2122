package protocols.apps.kademlia;

import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public abstract class KademliaMessage extends ProtoMessage{
	
	
	public KademliaMessage(short id) {
		super(id);
		// TODO Auto-generated constructor stub
	}

	public abstract byte[] toByteArray();
}
