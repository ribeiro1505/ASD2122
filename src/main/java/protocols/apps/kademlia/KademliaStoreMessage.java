package protocols.apps.kademlia;

import java.io.IOException;
import java.math.BigInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class KademliaStoreMessage extends ProtoMessage{

	public static final short MSG_ID = 705;
	private final KademliaNode sender;
	private final BigInteger target;
	private final BigInteger messageId;
	private byte[] payload;
	
	public KademliaStoreMessage(KademliaNode sender, BigInteger target, BigInteger messageId, byte[] payload) {
		super(MSG_ID);
		this.sender = sender;
		this.target = target;
		this.messageId = messageId;
		this.payload = payload;
	}
	
	public KademliaNode getSender() {
		return this.sender;
	}
	
	public BigInteger getTargetId() {
		return this.target;
	}
	
	public BigInteger getMessageId() {
		return this.messageId;
	}
	
	public byte[] getPlayload() {
		return this.payload;
	}
	
	// |MSG_TYPE|SENDER_ID|HOST|TARGET|MSG_ID|PAYLOAD|
	public static ISerializer<KademliaStoreMessage> serializer = new ISerializer<>() {
		@Override
		public void serialize(KademliaStoreMessage msg, ByteBuf out) throws IOException {
			out.writeShort(MSG_ID);
			out.writeBytes(msg.getSender().getKey().toByteArray());
			Host.serializer.serialize(msg.getSender().getHost(), out);
			out.writeBytes(msg.getTargetId().toByteArray());
			out.writeBytes(msg.getMessageId().toByteArray());
			out.writeBytes(msg.getPlayload());
		}
		
		@Override
		public KademliaStoreMessage deserialize(ByteBuf in) throws IOException {
			short msgType = in.readShort();
			byte[] buffer = new byte[20];
			in.readBytes(buffer);
			BigInteger senderId = new BigInteger(buffer);
			Host host = Host.serializer.deserialize(in);
			in.readBytes(buffer);
			BigInteger targetId = new BigInteger(buffer);
			in.readBytes(buffer);
			BigInteger messageId = new BigInteger(buffer);
			ByteBuf b = Unpooled.buffer();
			in.readBytes(b);
			byte[] payload = b.array();
			return new KademliaStoreMessage(new KademliaNode(senderId, host), targetId, messageId, payload);
		}
	};
}
