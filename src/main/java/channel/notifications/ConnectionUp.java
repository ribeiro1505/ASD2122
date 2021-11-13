package channel.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.Set;

public class ConnectionUp extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final Set<Host> neighbours;

    public ConnectionUp(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbours = new HashSet<>();
        neighbours.add(neighbour);
    }
    
    public void addNeighbour(Host neighbour) {
    	this.neighbours.add(neighbour);
    }

    public Set<Host> getNeighbours() {
        return new HashSet<>(this.neighbours);
    }
    
    public int getLength() {
    	return this.neighbours.size();
    }
}