package org.apache.shardingsphere.driver.jdbc.core.resultset;

import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;

import java.io.InputStream;
import java.io.Reader;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;

/**
 * QueryResult 包装器，解决 LocalDateTime 与 Timestamp 转换问题
 */
// [Custom Modification]: Add QueryResultWrapper
public class QueryResultWrapper implements QueryResult {

    public static QueryResultWrapper wrap(QueryResult queryResult) {
        if (queryResult instanceof QueryResultWrapper) {
            return (QueryResultWrapper) queryResult;
        }
        return new QueryResultWrapper(queryResult);
    }

    /**
     * 委托查询结果对象
     */
    private final QueryResult delegate;
    /**
     * 路由执行单元
     */
    private ExecutionUnit routeUnit;

    private QueryResultWrapper(QueryResult queryResult) {
        this.delegate = queryResult;
    }

    /**
     * 设置查询结果来源路由（数据源和真实表）
     */
    public QueryResultWrapper withRouteUnit(ExecutionUnit routeUnit) {
        this.routeUnit = routeUnit;
        return this;
    }

    /**
     * 数据源名称
     */
    public String getDataSourceName() {
        return this.routeUnit.getDataSourceName();
    }

    /**
     * 逻辑表名称
     */
    public String getLogicTableName() {
        return this.routeUnit.getSqlUnit().getTableRouteMappers().get(0).getLogicName();
    }

    /**
     * 真实表名称（scheme.table形式）
     */
    public String getActualTableName() {
        return this.routeUnit.getSqlUnit().getTableRouteMappers().get(0).getActualName();
    }

    /**
     * 实际执行的SQL语句
     */
    public String getActualSql() {
        return this.routeUnit.getSqlUnit().getSql();
    }

    @Override
    public boolean next() throws SQLException {
        return this.delegate.next();
    }

    @Override
    public Object getValue(int columnIndex, Class<?> type) throws SQLException {
        return this.checkTimestamp(this.delegate.getValue(columnIndex, type), columnIndex, type);
    }

    @Override
    public Object getCalendarValue(int columnIndex, Class<?> type, Calendar calendar) throws SQLException {
        return this.checkTimestamp(this.delegate.getCalendarValue(columnIndex, type, calendar), columnIndex, type);
    }

    private Object checkTimestamp(Object result, int columnIndex, Class<?> type) throws SQLException {
        if (isDateTime(result, columnIndex, type)) {
            return Timestamp.from(((LocalDateTime) result).atOffset(ZoneOffset.ofHours(8)).toInstant());
        }
        return result;
    }

    private boolean isDateTime(Object result, int columnIndex, Class<?> type) throws SQLException {
        return result instanceof LocalDateTime
                && type == Object.class
                && this.getMetaData().getColumnType(columnIndex) == Types.TIMESTAMP;
    }

    @Override
    public InputStream getInputStream(int columnIndex, String type) throws SQLException {
        return this.delegate.getInputStream(columnIndex, type);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return this.delegate.getCharacterStream(columnIndex);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return this.delegate.wasNull();
    }

    @Override
    public QueryResultMetaData getMetaData() {
        return this.delegate.getMetaData();
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }

}
