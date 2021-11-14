package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.NodeList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaFindNodeResponse extends ProtoMessage{
	public static final short MSG_ID = 704;
	private final KademliaNode sender;
	private final BigInteger target;
	private final BigInteger messageId;
	private final BigInteger echoedId;
	private final List<KademliaNode> nodeList;

	public KademliaFindNodeResponse(KademliaNode sender, BigInteger target, BigInteger messageId, BigInteger echoedId, List<KademliaNode> nodes) {
		super(MSG_ID);
		this.sender = sender;
		this.target = target;
		this.messageId = messageId;
		this.echoedId = echoedId;
		this.nodeList = nodes;
	}
	
	public KademliaNode getSender() {
		return this.sender;
	}
	
	public BigInteger getTargetId() {
		return this.target;
	}
	
	public Host getHost() {
		return this.sender.getHost();
	}
	
	public BigInteger getMessageId() {
		return this.messageId;
	}
	
	public BigInteger getEchoedId() {
		return this.echoedId;
	}
	
	public int getNodeCount() {
		return nodeList.size();
	}
	
	public List<KademliaNode> getNodes(){
		return this.nodeList;
	}
	
	// |MSG_TYPE|SENDER_ID|TARGET_ID|MSG_ID|ECHOED_ID|HOST|NODE_COUNT|NODES...|
	public static ISerializer<KademliaFindNodeResponse> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaFindNodeResponse msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			out.writeBytes(msg.getTargetId().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getEchoedId().toByteArray());
			Host.serializer.serialize(msg.getHost(), out);
			out.writeInt(msg.getNodeCount());
			for(KademliaNode node : msg.getNodes()) {
				KademliaNode.serializer.serialize(node, out);
			}
		}
		
		@Override
		public KademliaFindNodeResponse deserialize(ByteBuf in) throws IOException {
			in.readShort();
			byte[] buffer = new byte[20];
			in.readBytes(buffer);
			BigInteger senderId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger targetId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger messageId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger echoedId = new BigInteger(buffer);
			Host host = Host.serializer.deserialize(in);
			int nodeCount = in.readInt();
			List<KademliaNode> nodes = new ArrayList<>();
			for(int i = 0; i < nodeCount; i++) {
				nodes.add(KademliaNode.serializer.deserialize(in));
			}
			return new KademliaFindNodeResponse(new KademliaNode(senderId, host), targetId, messageId, echoedId, nodes);
		}
	};
	
	public static void main(String[] args) throws Exception{
		Host host = new Host(InetAddress.getByName("127.0.0.1"), 1234);
		KademliaNode node = new KademliaNode(host);
		List<KademliaNode> nodes = new ArrayList<KademliaNode>();
		nodes.add(node);
		nodes.add(node);
		nodes.add(node);
		
		KademliaFindNodeResponse kad = new KademliaFindNodeResponse(node, node.getKey(), node.getKey(), node.getKey(), nodes);
		
		System.out.println(kad.getSender());
		System.out.println(kad.getTargetId());
		System.out.println(kad.getMessageId());
		System.out.println(kad.getEchoedId());
		System.out.println(kad.getNodeCount());
		for(KademliaNode n : kad.getNodes()) {
			System.out.println(n);
		}
		System.out.println();
		
		ByteBuf buf = Unpooled.buffer();
		KademliaFindNodeResponse.serializer.serialize(kad, buf);
		
		KademliaFindNodeResponse kad2 = KademliaFindNodeResponse.serializer.deserialize(buf);
		
		System.out.println(kad2.getSender());
		System.out.println(kad2.getTargetId());
		System.out.println(kad2.getMessageId());
		System.out.println(kad2.getEchoedId());
		System.out.println(kad2.getNodeCount());
		for(KademliaNode n : kad2.getNodes()) {
			System.out.println(n);
		}
	}
}
