/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel

    /**
     * 读取数据时，可能会存在粘包和拆包的问题，解决方案在下列方法中：
     * 粘包问题：如何区分哪段数据是A响应，哪段数据是B响应？
     *     答案：kafka在每个响应前面插入了一个默认4个字节，也就是一个int的所占空间大小，表示响应数据的大小
     *     举例：响应1 响应数据200字节 响应2 响应数据121个字节 响应3 20个字节
     *          从channel中读取ByteBuffer size(4个字节)
     *              如果读满，调用size.rewind()，把position设置为0，此时就可以从ByteBuffer size中读取数据，
     *              读取出来的数据就是表示后续响应数据的长度，从而解决粘包问题
     *  拆包问题：size没有读满或者数据没有读完，200个字节只读取了166个
     *     答案：1.size没有读满
     *     举例：4个字节size，只读取了3个字节
     *           此种情况if (!size.hasRemaining())方法并不会执行，也就是buffer依旧为null，从而导致if (buffer != null)也不会执行，
     *           上层调用方中的 if (receive.complete())方法返回false，导致KafkaChannel类中的属性receive不会被置为null，那么在外层
     *           的while循环，会再次执行进来，此时的receive依旧和上一次的是同一个，然后继续读取，直到size读取完4个字节
     *     答案：2.数据没有读满
     *     举例：200个字节只读取了166个
     *           那么只需要再次等待下次的OP_READ事件即可
     */
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        // 累计读取总的字节数
        int read = 0;
        // 判断当前NetworkReceive的size(默认4个字节，也就是一个int的所占空间大小)是否有剩余空间
        if (size.hasRemaining()) {
            // 读取size个字节的数据
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            // 如果size没满，发生了拆包
            // 如果size读满，发生了粘包，只需要在读取对应size所表示的字节数即可
            if (!size.hasRemaining()) {
                size.rewind();
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

}
