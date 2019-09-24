/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.core.codec;

/**
 * The enum serialize type.
 *
 * @author leizhiyuan
 * 编解码枚举
 * 看来这种分布式内部通信都是不使用 json 作为序列化方式的 而是追求更高的性能
 */
public enum CodecType {

    /**
     * The seata.
     * <p>
     * Math.pow(2, 0)
     * 应该是 seata 自定义的某种序列化方式
     */
    SEATA((byte)0x1),

    /**
     * The protobuf.
     * <p>
     * Math.pow(2, 1)
     * google 的 protobuf
     */
    PROTOBUF((byte)0x2),

    /**
     * The kryo.
     * <p>
     * Math.pow(2, 2)
     * kryo
     */
    KRYO((byte)0x4),
    ;

    private final byte code;

    CodecType(final byte code) {
        this.code = code;
    }

    /**
     * Gets result code.
     *
     * @param code the code
     * @return the result code
     */
    public static CodecType getByCode(int code) {
        for (CodecType b : CodecType.values()) {
            if (code == b.code) {
                return b;
            }
        }
        throw new IllegalArgumentException("unknown codec:" + code);
    }

    /**
     * Gets result code.
     *
     * @param name the name
     * @return the result code
     */
    public static CodecType getByName(String name) {
        for (CodecType b : CodecType.values()) {
            if (b.name().equalsIgnoreCase(name)) {
                return b;
            }
        }
        throw new IllegalArgumentException("unknown codec:" + name);
    }

    /**
     * Gets code.
     *
     * @return the code
     */
    public byte getCode() {
        return code;
    }
}
