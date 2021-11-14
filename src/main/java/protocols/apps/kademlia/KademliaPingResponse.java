package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaPingResponse extends ProtoMessage{

	public static final short MSG_ID = 702;
	private final KademliaNode sender;
	private final BigInteger messageId;
	private final BigInteger echoedId;
	
	public KademliaPingResponse(KademliaNode sender, BigInteger messageId, BigInteger echoedId) {
		super(MSG_ID);
		this.sender = sender;
		this.messageId = messageId;
		this.echoedId = echoedId;
	}
	
	public KademliaNode getSender() {
		return this.sender;
	}
	
	public BigInteger getMessageId() {
		return this.messageId;
	}
	
	public BigInteger getEchoedId() {
		return this.echoedId;
	}
	
	public static ISerializer<KademliaPingResponse> serializer = new ISerializer<>() {

		@Override
		public void serialize(KademliaPingResponse msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getEchoedId().toByteArray());
			Host.serializer.serialize(msg.getSender().getHost(), out);
		}

		@Override
		public KademliaPingResponse deserialize(ByteBuf in) throws IOException {
			short messageType = in.readShort();
			byte[] buf = new byte[20];
			in.readBytes(buf);
			BigInteger senderId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger messageId = new BigInteger(buf);
			in.readBytes(buf);
			BigInteger echoedId = new BigInteger(buf);
			Host host = Host.serializer.deserialize(in);
			KademliaNode node = new KademliaNode(senderId, host);
			return new KademliaPingResponse(node, messageId, echoedId);
		}
	};
	
	public static void main(String[] args) throws Exception {
		// There a bug that if you use localhost instead of 127.0.0.1 it screws up the byte alingment
		KademliaNode node = new KademliaNode(new Host(InetAddress.getByName("127.0.0.1"), 1234));
		KademliaPingResponse res = new KademliaPingResponse(node, node.getKey(), node.getKey());
		System.out.println(res.getSender());
		System.out.println(res.getMessageId());
		System.out.println(res.getEchoedId());
		ByteBuf buf = Unpooled.buffer();
		KademliaPingResponse.serializer.serialize(res, buf);
		KademliaPingResponse res2 = KademliaPingResponse.serializer.deserialize(buf);
		System.out.println(res2.getSender());
		System.out.println(res2.getMessageId());
		System.out.println(res2.getEchoedId());
		
	}
}
