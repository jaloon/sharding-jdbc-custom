package org.apache.shardingsphere.driver.jdbc.core.connection;

/**
 * 连接接口
 */
// [Custom Modification]: Add Connectable
public interface Connectable {
    /**
     * 是否可连接
     */
    boolean isConnected();

    /**
     * 检查连接状态
     */
    void checkConnected();
}
