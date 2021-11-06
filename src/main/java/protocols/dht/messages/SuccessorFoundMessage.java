package protocols.dht.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.UUID;


public class SuccessorFoundMessage extends ProtoMessage{

    public static final short MSG_ID = 102;

    private final UUID mid;
    private final Host successor;
    private final int ofNode;

    private final short toDeliver;


    public SuccessorFoundMessage(UUID mid, Host successor,int ofNode, short toDeliver) {
        super(MSG_ID);
        this.mid = mid;
        this.successor = successor;
        this.ofNode=ofNode;
        this.toDeliver = toDeliver;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSuccessor() {
        return successor;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public int getOfNode() {
        return ofNode;
    }

    public static ISerializer<SuccessorFoundMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(SuccessorFoundMessage findSuccessorMessage, ByteBuf out) throws IOException {
            out.writeLong(findSuccessorMessage.mid.getMostSignificantBits());
            out.writeLong(findSuccessorMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(findSuccessorMessage.successor, out);
            out.writeShort(findSuccessorMessage.toDeliver);
            out.writeInt(findSuccessorMessage.ofNode);


            /*out.writeInt(floodMessage.content.length);
            if (floodMessage.content.length > 0) {
                out.writeBytes(floodMessage.content);
            }*/
        }


        @Override
        public SuccessorFoundMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sucessor = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int ofNode = in.readInt();

            return new SuccessorFoundMessage(mid, sucessor,ofNode, toDeliver);
        }
    };
}