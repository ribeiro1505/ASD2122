package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import protocols.storage.messages.RetrieveMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.util.UUID;

public class FindSuccessorMessage extends ProtoMessage {

    public static final short MSG_ID = 101;

    private final UUID mid;
    private final Host sender;
    private final Host ofNode;

    private final short toDeliver;


    public FindSuccessorMessage(UUID mid, Host sender,Host ofNode, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.ofNode=ofNode;
        this.toDeliver = toDeliver;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public Host getOfNode() {
        return ofNode;
    }

    public static ISerializer<FindSuccessorMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(FindSuccessorMessage findSuccessorMessage, ByteBuf out) throws IOException {
            out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
            out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(findSuccessorMessage.sender, out);
            out.writeShort(findSuccessorMessage.toDeliver);
            Host.serializer.serialize(findSuccessorMessage.ofNode, out);


            /*out.writeInt(floodMessage.content.length);
            if (floodMessage.content.length > 0) {
                out.writeBytes(floodMessage.content);
            }*/
        }


        @Override
        public FindSuccessorMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            Host ofNode = Host.serializer.deserialize(in);

            return new FindSuccessorMessage(mid, sender,ofNode, toDeliver);
        }
    };
}
