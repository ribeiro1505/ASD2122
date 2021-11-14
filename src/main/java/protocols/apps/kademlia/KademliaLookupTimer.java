package protocols.apps.kademlia;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class KademliaLookupTimer extends ProtoTimer{

	public static final short TIMER_ID = 701;
	
	public KademliaLookupTimer() {
		super(TIMER_ID);
	}
	
	@Override
    public ProtoTimer clone() {
        return this;
    }
}
