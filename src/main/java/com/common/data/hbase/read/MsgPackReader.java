package com.common.data.hbase.read;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.buffer.ArrayBufferInput;
import org.msgpack.core.buffer.MessageBuffer;
import org.msgpack.core.buffer.MessageBufferInput;

import java.io.IOException;

/**
 * 反序列化msgpack
 * 支持int、long、boolean、short、float、double，支持String和byte[]
 * NOT thread safe
 * 用完需要关闭
 * Created by smyang on 16/6/1.
 */
class MsgPackReader extends MessageUnpacker {

    private ArrayBufferInput buffer;

    public MsgPackReader(ArrayBufferInput buffer) {
        super(buffer, MessagePack.DEFAULT_UNPACKER_CONFIG);
        this.buffer = buffer;
    }

    /**
     * 这种方法创建的对象是暂时没有内容的，需要调用{@link #reset(byte[])}方法，然后才能解压
     *
     * @return 全新的实例
     */
    public static MsgPackReader create() {
        ArrayBufferInput buffer = new ArrayBufferInput((MessageBuffer) null);
        return new MsgPackReader(buffer);
    }

    /**
     * 这种创建方法调用之后可以解压
     *
     * @param src 需要解压的数据
     * @return 全新的实例
     */
    public static MsgPackReader create(byte[] src) {
        ArrayBufferInput buffer = new ArrayBufferInput(src);
        return new MsgPackReader(buffer);
    }

    /**
     * 重置为初始状态，接收src的数据准备从0开始解压
     *
     * @param src 需要解压的数据
     * @return this
     * @throws IOException {@link MessageUnpacker#reset(MessageBufferInput)}
     */
    public MsgPackReader reset(byte[] src) throws IOException {
        buffer.reset(src);
        super.reset(buffer);
        return this;
    }

    public byte[] unpackByteArray() throws IOException {
        int len = unpackBinaryHeader();
        return readPayload(len);
    }

    public int getInt(byte[] s) throws IOException {
        return reset(s).unpackInt();
    }

    public long getLong(byte[] s) throws IOException {
        return reset(s).unpackLong();
    }

    public boolean getBoolean(byte[] s) throws IOException {
        return reset(s).unpackBoolean();
    }

    public short getShort(byte[] s) throws IOException {
        return reset(s).unpackShort();
    }

    public float getFloat(byte[] s) throws IOException {
        return reset(s).unpackFloat();
    }

    public double getDouble(byte[] s) throws IOException {
        return reset(s).unpackDouble();
    }

    public String getString(byte[] s) throws IOException {
        return reset(s).unpackString();
    }

    public Object unpack(Class<?> clazz) throws IOException {
        if (clazz == int.class) {
            return unpackInt();
        } else if (clazz == float.class) {
            return unpackFloat();
        } else if (clazz == long.class) {
            return unpackLong();
        } else if (clazz == short.class) {
            return unpackShort();
        } else if (clazz == double.class) {
            return unpackDouble();
        } else if (clazz == boolean.class) {
            return unpackBoolean();
        } else if (clazz == String.class) {
            return unpackString();
        } else if (clazz == byte[].class) {
            return unpackByteArray();
        } else {
            throw new IOException("Can NOT unpack the class " + clazz.getCanonicalName());
        }
    }

}
