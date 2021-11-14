package protocols.apps.kademlia;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class KademliaBucket {

	private LinkedList<KademliaNode> nodes;
	private final int capacity;
	private int distance;
	private List<KademliaNode> staleCache;
	private int staleCount;
	
	public KademliaBucket(int maxCapacity, int distance) {
		this.capacity = maxCapacity;
		this.distance = distance;
		this.nodes = new LinkedList<>();
		this.staleCache = new LinkedList<>();
		this.staleCount = 0;
	}
	
	public int getCapacity() {
		return this.capacity;
	}
	
	public int getHowManyNodes() {
		return this.nodes.size();
	}
	
	public int getFreeSpots() {
		return capacity - nodes.size();
	}
	
	public boolean hasSpace() {
		return (getFreeSpots() > 0);
	}
	
	public KademliaNode peekFirst() {
		return nodes.getFirst();
	}
	
	public KademliaNode popFirst() {
		return nodes.pop();
	}
	
	public void pushLast(KademliaNode node) {
		nodes.addLast(node);
	}
	
	/*
	 * Returns the index o the node/contact if it exists, -1 otherwise.
	 */
	public int findContact(KademliaNode node) {
		for(int i = 0; i < nodes.size(); i++) {
			if(nodes.get(i).getKey().compareTo(node.getKey()) == 0) {
				return i;
			}
		}
		return -1;
	}
	
	public int findContact(BigInteger key) {
		for(int i = 0; i < nodes.size(); i++) {
			if(nodes.get(i).getKey().compareTo(key) == 0) {
				return i;
			}
		}
		return -1;
	}
	
	public KademliaNode getContact(BigInteger id) {
		for(KademliaNode n : nodes) {
			if(n.getKey().compareTo(id)==0)
				return n;
		}
		return null;
	}
	
	public List<KademliaNode> getNContacts(int number) {
		List<KademliaNode> ret = new ArrayList<KademliaNode>();
		if(number <= nodes.size()) {
			return nodes.subList(0, number);
		}
		return nodes;
	}
	
	public void insertContact(KademliaNode node) {
		int index = findContact(node);
		if(index >= 0) {
			nodes.remove(index);
		}
		nodes.addLast(node);
	}
}
