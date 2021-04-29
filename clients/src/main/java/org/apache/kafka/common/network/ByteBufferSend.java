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
import java.nio.channels.GatheringByteChannel;

/**
 * A send backed by an array of byte buffers
 */
public class ByteBufferSend implements Send {
    // 目标
    private final String destination;
    // buffers空间大小
    private final int size;
    // 待写入到channel的数据
    protected final ByteBuffer[] buffers;
    // buffers剩余空间,发送完毕后为0
    private int remaining;
    //
    private boolean pending = false;

    public ByteBufferSend(String destination, ByteBuffer... buffers) {
        super();
        this.destination = destination;
        this.buffers = buffers;
        for (int i = 0; i < buffers.length; i++)
            remaining += buffers[i].remaining();
        this.size = remaining;
    }

    @Override
    public String destination() {
        return destination;
    }

    /**
     * 判断是否发送完毕
     * 剩余空间<=0 并且
     * @return
     */
    @Override
    public boolean completed() {
        return remaining <= 0 && !pending;
    }

    @Override
    public long size() {
        return this.size;
    }

    /**
     * 写入消息到对应的channel
     * @param channel The Channel to write to
     * @return
     * @throws IOException
     */
    @Override
    public long writeTo(GatheringByteChannel channel) throws IOException {
        // 写消息到channel并返回写入的字节数
        long written = channel.write(buffers);
        if (written < 0)
            throw new EOFException("Wrote negative bytes to channel. This shouldn't happen.");
        // 计算剩余空间
        remaining -= written;
        // This is temporary workaround. As Send , Receive interfaces are being used by BlockingChannel.
        // Once BlockingChannel is removed we can make Send, Receive to work with transportLayer rather than
        // GatheringByteChannel or ScatteringByteChannel.
        if (channel instanceof TransportLayer)
            pending = ((TransportLayer) channel).hasPendingWrites();
        // 返回写入的字节数
        return written;
    }
}
