/**
 * Representation of a node in a Kademlia network.
 * Used to be encapsulation of related date to be stored in each nodes k-buckets.
 */

package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaNode {

	private final BigInteger key;
	private final Host host;
	private long lastSeen;

	public KademliaNode(BigInteger key, Host host) {
		this.key = key;
		this.host = host;
		this.lastSeen = System.currentTimeMillis() / 1000L;
	}

	public KademliaNode(Host host) throws NoSuchAlgorithmException {
		this(computeKey(host), host);
	}

	private static BigInteger computeKey(Host host) throws NoSuchAlgorithmException {
		byte[] target = (host.getAddress() + ":" + host.getPort()).getBytes();
		byte[] hash = MessageDigest.getInstance("SHA-1").digest(target);
		return new BigInteger(1, hash);
	}

	public BigInteger getKey() {
		return this.key;
	}

	public Host getHost() {
		return this.host;
	}

	public long getLastSeen() {
		return this.lastSeen;
	}

	public void updateLastSeen(long newTimestamp) {
		this.lastSeen = newTimestamp;
	}

	public void updateLastSeen() {
		this.lastSeen = System.currentTimeMillis() / 1000L;
	}

	public BigInteger getDistance(KademliaNode other) {
		return this.key.xor(other.getKey()).abs();
	}
	
	public BigInteger getDistance(BigInteger key) {
		return this.key.xor(key).abs();
	}

	@Override
	public String toString() {
		return "[key=" + key.toString(16) + ",host=" + host.getAddress() + ":" + host.getPort() + "]";
	}
	
	// |NODE_ID|HOST|
	public static ISerializer<KademliaNode> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaNode node, ByteBuf out) throws IOException {
			out.writeBytes(node.getKey().toByteArray());
			Host.serializer.serialize(node.getHost(), out);
		}
		
		@Override
		public KademliaNode deserialize(ByteBuf in) throws IOException {
			byte[] buffer = new byte[20];
			in.readBytes(buffer);
			BigInteger key = new BigInteger(buffer);
			Host host = Host.serializer.deserialize(in);
			return new KademliaNode(key, host);
		}
	};
	
	public static void main(String[] args) throws Exception{
		Host host = new Host(InetAddress.getByName("127.0.0.1"), 1234);
		KademliaNode node = new KademliaNode(host);
		
		System.out.println(node.getKey());
		System.out.println(node.getHost());
		
		ByteBuf buf = Unpooled.buffer();
		KademliaNode.serializer.serialize(node, buf);
		
		KademliaNode node2 = KademliaNode.serializer.deserialize(buf);
		
		System.out.println(node2.getKey());
		System.out.println(node2.getHost());
	}
}
