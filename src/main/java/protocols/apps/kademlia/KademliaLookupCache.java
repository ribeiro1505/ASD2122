package protocols.apps.kademlia;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class KademliaLookupCache {

	private final BigInteger target;
	private final KademliaNode requester;
	private Map<BigInteger, Boolean> searched;
	private TreeSet<KademliaNode> nodes;
	
	public KademliaLookupCache(KademliaNode requester, BigInteger target, List<KademliaNode> nodes) {
		this.requester = requester;
		this.target = target;
		this.nodes = new TreeSet<KademliaNode>(new Comparator<KademliaNode>() {
			@Override
			public int compare(KademliaNode o1, KademliaNode o2) {
				return o2.getDistance(target).compareTo(o1.getDistance(target));
			}
		});
		this.nodes.addAll(nodes);
		this.searched = new HashMap<BigInteger, Boolean>();
	}
	
	public KademliaNode getRequester() {
		return this.requester;
	}
	
	public BigInteger getTarget() {
		return this.target;
	}
	
	public KademliaNode hasFound() {
		for(KademliaNode n : nodes) {
			if(n.getKey().compareTo(target) == 0);
			return n;
		}
		return null;
	}
	
	public TreeSet<KademliaNode> getNodes(){
		return this.nodes;
	}
	
	public KademliaNode popNode() {
		if(hasFound() != null)
			return null;
		KademliaNode node = nodes.pollFirst();
		searched.put(node.getKey(), true);
		return node;
	}
	
	public void addNode(KademliaNode n) {
		if(hasFound() != null)
			return;
		if(!searched.getOrDefault(n.getKey(), false)) {
			nodes.add(n);
		}
	}
	
	
}
