package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaFindNodeMessage extends ProtoMessage{
	public static short MSG_ID = 703;
	
	private final KademliaNode sender;
	private final BigInteger target;
	private final BigInteger messageId;
	
	public KademliaFindNodeMessage(KademliaNode sender, BigInteger targetId, BigInteger messageId) {
		super(MSG_ID);
		this.sender = sender;
		this.target = targetId;
		this.messageId =  messageId;
	}
	
	public KademliaNode getSender() {
		return this.sender;
	}
	
	public BigInteger getTarget() {
		return this.target;
	}
	
	public BigInteger getMessageId() {
		return this.messageId;
	}
	
	public static ISerializer<KademliaFindNodeMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaFindNodeMessage msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getTarget().toByteArray());
			Host.serializer.serialize(msg.getSender().getHost(), out);
		}

		@Override
		public KademliaFindNodeMessage deserialize(ByteBuf in) throws IOException {
			short messageType = in.readShort();
			byte[] buf = new byte[20];
			in.readBytes(buf);
			BigInteger senderId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger messageId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger target = new BigInteger(buf);
			Host host = Host.serializer.deserialize(in);
			KademliaNode node = new KademliaNode(senderId, host);
			return new KademliaFindNodeMessage(node, messageId, target);
		}
	};
	
	public static void main(String[] args) throws Exception{
		Host host = new Host(InetAddress.getByName("127.0.0.1"), 1234);
		KademliaNode node = new KademliaNode(host);
		KademliaFindNodeMessage ping = new KademliaFindNodeMessage(node, node.getKey(), node.getKey());
		System.out.println(ping.getSender());
		System.out.println(ping.getMessageId());
		System.out.println(ping.getTarget());
		
		ByteBuf buf = Unpooled.buffer();
		KademliaFindNodeMessage.serializer.serialize(ping, buf);
		
		KademliaFindNodeMessage ping2 = KademliaFindNodeMessage.serializer.deserialize(buf);
		System.out.println(ping2.getSender());
		System.out.println(ping2.getMessageId());
		System.out.println(ping2.getTarget());
	}
}
