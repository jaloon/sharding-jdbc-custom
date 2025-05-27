/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.infra.executor.sql.execute.engine.driver.jdbc;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.annotation.HighFrequencyInvocation;
import org.apache.shardingsphere.infra.database.core.connector.ConnectionProperties;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.kernel.model.ExecutorCallback;
import org.apache.shardingsphere.infra.executor.sql.context.SQLUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutorExceptionHandler;
import org.apache.shardingsphere.infra.executor.sql.hook.SPISQLExecutionHook;
import org.apache.shardingsphere.infra.executor.sql.hook.SQLExecutionHook;
import org.apache.shardingsphere.infra.executor.sql.process.ProcessEngine;
import org.apache.shardingsphere.infra.metadata.database.resource.ResourceMetaData;
import org.apache.shardingsphere.infra.metadata.database.resource.unit.StorageUnit;
import org.apache.shardingsphere.sql.parser.statement.core.statement.SQLStatement;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Optional;

/**
 * JDBC executor callback.
 *
 * @param <T> class type of return value
 */
@Slf4j
@HighFrequencyInvocation
@RequiredArgsConstructor
public abstract class JDBCExecutorCallback<T> implements ExecutorCallback<JDBCExecutionUnit, T> {
    // [Custom Modification]: modify fields access level to protected

    protected final DatabaseType protocolType;

    protected final ResourceMetaData resourceMetaData;

    protected final SQLStatement sqlStatement;

    protected final boolean isExceptionThrown;

    protected final ProcessEngine processEngine = new ProcessEngine();

    // [Custom Modification]: remove final modifier
    @Override
    public Collection<T> execute(final Collection<JDBCExecutionUnit> executionUnits, final boolean isTrunkThread, final String processId) throws SQLException {
        // TODO It is better to judge whether need sane result before execute, can avoid exception thrown
        Collection<T> result = new LinkedList<>();
        for (JDBCExecutionUnit each : executionUnits) {
            T executeResult = execute(each, isTrunkThread, processId);
            if (null != executeResult) {
                result.add(executeResult);
            }
        }
        return result;
    }
    
    /*
     * To make sure SkyWalking will be available at the next release of ShardingSphere, a new plugin should be provided to SkyWalking project if this API changed.
     *
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     */
    // [Custom Modification]: modify method access level to protected
    protected T execute(final JDBCExecutionUnit jdbcExecutionUnit, final boolean isTrunkThread, final String processId) throws SQLException {
        SQLExecutorExceptionHandler.setExceptionThrown(isExceptionThrown);
        String dataSourceName = jdbcExecutionUnit.getExecutionUnit().getDataSourceName();
        // TODO use metadata to replace storageUnits to support multiple logic databases
        StorageUnit storageUnit = resourceMetaData.getStorageUnits().containsKey(dataSourceName)
                ? resourceMetaData.getStorageUnits().get(dataSourceName)
                : resourceMetaData.getStorageUnits().values().iterator().next();
        DatabaseType storageType = storageUnit.getStorageType();
        ConnectionProperties connectionProps = storageUnit.getConnectionProperties();
        SQLExecutionHook sqlExecutionHook = new SPISQLExecutionHook();
        SQLUnit sqlUnit = jdbcExecutionUnit.getExecutionUnit().getSqlUnit();
        String sql = sqlUnit.getSql();
        Statement statement = jdbcExecutionUnit.getStorageResource();
        try {
            sqlExecutionHook.start(dataSourceName, sql, sqlUnit.getParameters(), connectionProps, isTrunkThread);
            T result = executeSQL(sql, statement, jdbcExecutionUnit.getConnectionMode(), storageType);
            sqlExecutionHook.finishSuccess();
            processEngine.completeSQLUnitExecution(jdbcExecutionUnit, processId);
            // log.info("[{}({})]Exec route sql unit success, sql: {}\n\tstmt: {}", storageType.getType(), dsName, sql, statement);
            return result;
        } catch (final SQLException | RuntimeException | Error e) {
            // [Custom Modification]: 增加路由单元sql执行错误日志，便于排查具体执行哪个数据源的哪条sql出错
            log.error("[{}({})]Exec route sql unit error, sql: {}\n\tstmt: {}", storageType.getType(), dataSourceName, sql, statement);
            if (e instanceof RuntimeException || e instanceof Error) {
                throw e;
            }
            SQLException ex = (SQLException) e;
            if (!storageType.equals(protocolType)) {
                Optional<T> saneResult = getSaneResult(sqlStatement, ex);
                if (saneResult.isPresent()) {
                    return isTrunkThread ? saneResult.get() : null;
                }
            }
            sqlExecutionHook.finishFailure(ex);
            SQLExecutorExceptionHandler.handleException(ex);
            return null;
        }
    }
    
    protected abstract T executeSQL(String sql, Statement statement, ConnectionMode connectionMode, DatabaseType storageType) throws SQLException;
    
    protected abstract Optional<T> getSaneResult(SQLStatement sqlStatement, SQLException ex);
}
