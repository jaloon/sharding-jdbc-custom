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

package org.apache.shardingsphere.infra.binder.engine.segment.dml.from.type;

import com.cedarsoftware.util.CaseInsensitiveMap.CaseInsensitiveString;
import com.google.common.collect.Multimap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.TableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.segment.dml.from.context.type.SimpleTableSegmentBinderContext;
import org.apache.shardingsphere.infra.binder.engine.segment.util.SubqueryTableBindUtils;
import org.apache.shardingsphere.infra.binder.engine.statement.SQLStatementBinderContext;
import org.apache.shardingsphere.infra.database.core.metadata.database.DialectDatabaseMetaData;
import org.apache.shardingsphere.infra.database.core.metadata.database.enums.QuoteCharacter;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.database.core.type.DatabaseTypeRegistry;
import org.apache.shardingsphere.infra.database.opengauss.type.OpenGaussDatabaseType;
import org.apache.shardingsphere.infra.database.postgresql.type.PostgreSQLDatabaseType;
import org.apache.shardingsphere.infra.exception.core.ShardingSpherePreconditions;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.database.NoDatabaseSelectedException;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.database.UnknownDatabaseException;
import org.apache.shardingsphere.infra.exception.dialect.exception.syntax.table.TableExistsException;
import org.apache.shardingsphere.infra.exception.kernel.metadata.SchemaNotFoundException;
import org.apache.shardingsphere.infra.exception.kernel.metadata.TableNotFoundException;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.metadata.database.schema.manager.SystemSchemaManager;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereColumn;
import org.apache.shardingsphere.infra.metadata.database.schema.model.ShardingSphereSchema;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.column.ColumnDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.ddl.table.RenameTableDefinitionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.OwnerSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.ColumnSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.bound.TableSegmentBoundInfo;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.statement.core.segment.generic.table.TableNameSegment;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.AlterTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.AlterViewStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.CreateViewStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DropTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.DropViewStatement;
import org.apache.shardingsphere.sql.parser.statement.core.statement.ddl.RenameTableStatement;
import org.apache.shardingsphere.sql.parser.statement.core.util.SQLUtils;
import org.apache.shardingsphere.sql.parser.statement.core.value.identifier.IdentifierValue;

import java.util.*;

/**
 * Simple table segment binder.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SimpleTableSegmentBinder {
    
    private static final String PG_CATALOG = "pg_catalog";
    
    /**
     * Bind simple table segment.
     *
     * @param segment simple table segment
     * @param binderContext SQL statement binder context
     * @param tableBinderContexts table binder contexts
     * @return bound simple table segment
     */
    public static SimpleTableSegment bind(final SimpleTableSegment segment, final SQLStatementBinderContext binderContext,
                                          final Multimap<CaseInsensitiveString, TableSegmentBinderContext> tableBinderContexts) {
        fillPivotColumnNamesInBinderContext(segment, binderContext);
        // [Custom Modification]: Optimize table check logic
        // IdentifierValue databaseName = getDatabaseName(segment, binderContext);
        // IdentifierValue schemaName = getSchemaName(segment, binderContext, databaseName);
        DatabaseTypeRegistry registry = new DatabaseTypeRegistry(binderContext.getSqlStatement().getDatabaseType());
        // Get and check database
        ShardingSphereDatabase database = getAndCheckDatabase(segment, registry, binderContext);
        // Get and check schema
        ShardingSphereSchema schema = getSchemaAndCheckTableExists(segment, registry, binderContext, database);
        IdentifierValue schemaId = new IdentifierValue(schema.getName());
        boolean hasDefaultSchema = registry.getDialectDatabaseMetaData().getDefaultSchema().isPresent();
        IdentifierValue databaseId = hasDefaultSchema ? new IdentifierValue(database.getName()) : schemaId;
        // reset table owner
        if (!segment.getOwner().isPresent() && !PG_CATALOG.equals(schema.getName())) {
            int ownerIndex = segment.getTableName().getStartIndex();
            OwnerSegment owner = new OwnerSegment(ownerIndex, ownerIndex, schemaId);
            if (hasDefaultSchema) {
                owner.setOwner(new OwnerSegment(ownerIndex, ownerIndex, databaseId));
            }
            segment.setOwner(owner);
        }
        IdentifierValue tableName = segment.getTableName().getIdentifier();
        // ShardingSphereSchema schema = binderContext.getMetaData().getDatabase(databaseName.getValue()).getSchema(schemaName.getValue());
        // checkTableExists(binderContext, schema, schemaName.getValue(), tableName.getValue());
        TableSegmentBoundInfo boundInfo = new TableSegmentBoundInfo(databaseId, schemaId);
        tableBinderContexts.put(new CaseInsensitiveString(segment.getAliasName().orElseGet(tableName::getValue)),
                createSimpleTableBinderContext(segment, registry, schema, tableName, boundInfo, binderContext));
        TableNameSegment tableNameSegment = new TableNameSegment(segment.getTableName().getStartIndex(), segment.getTableName().getStopIndex(), tableName);
        tableNameSegment.setTableBoundInfo(boundInfo);
        SimpleTableSegment result = new SimpleTableSegment(tableNameSegment);
        segment.getOwner().ifPresent(result::setOwner);
        segment.getAliasSegment().ifPresent(result::setAlias);
        return result;
    }
    
    private static void fillPivotColumnNamesInBinderContext(final SimpleTableSegment segment, final SQLStatementBinderContext binderContext) {
        segment.getPivot().ifPresent(optional -> optional.getPivotColumns().forEach(each -> binderContext.getPivotColumnNames().add(each.getIdentifier().getValue())));
    }

    private static ShardingSphereDatabase getAndCheckDatabase(final SimpleTableSegment segment,
                                                              final DatabaseTypeRegistry registry,
                                                              final SQLStatementBinderContext binderContext) {
        Optional<OwnerSegment> owner = registry.getDialectDatabaseMetaData().getDefaultSchema().isPresent()
                ? segment.getOwner().flatMap(OwnerSegment::getOwner)
                : segment.getOwner();
        String databaseName = SQLUtils.getExactlyValue(owner
                .map(OwnerSegment::getIdentifier)
                .map(IdentifierValue::getValue)
                .orElseGet(binderContext::getCurrentDatabaseName));
        ShardingSpherePreconditions.checkNotNull(databaseName, NoDatabaseSelectedException::new);
        ShardingSphereDatabase database = binderContext.getMetaData().getDatabase(databaseName);
        ShardingSpherePreconditions.checkNotNull(database, () -> new UnknownDatabaseException(databaseName));
        return database;
    }

    private static ShardingSphereSchema getSchemaAndCheckTableExists(final SimpleTableSegment segment,
                                                                    final DatabaseTypeRegistry registry,
                                                                    final SQLStatementBinderContext binderContext,
                                                                    final ShardingSphereDatabase database) {
        String table = segment.getTableName().getIdentifier().getValue();
        // Get schema name from table owner
        if (segment.getOwner().isPresent()) {
            IdentifierValue owner = segment.getOwner().get().getIdentifier();
            ShardingSphereSchema schema = getAndCheckSchema(owner.getValue(), database);
            checkTableExists(binderContext, schema, table);
            return schema;
        }
        // Get PG system schema
        DatabaseType databaseType = database.getProtocolType();
        if ((databaseType instanceof PostgreSQLDatabaseType || databaseType instanceof OpenGaussDatabaseType)
                && SystemSchemaManager.isSystemTable(databaseType.getType(), PG_CATALOG, table)) {
            return getAndCheckSchema(PG_CATALOG, database);
        }
        // Get schema name according to search path
        for (ShardingSphereSchema schema : database.getAllSchemas()) {
            if (schema.containsTable(table)) {
                return schema;
            }
        }
        // Get default schema name
        String schemaName = registry.getDefaultSchemaName(binderContext.getCurrentDatabaseName());
        ShardingSphereSchema schema = getAndCheckSchema(SQLUtils.getExactlyValue(schemaName), database);
        checkTableExists(binderContext, schema, table);
        return schema;
    }

    private static ShardingSphereSchema getAndCheckSchema(String schemaName, final ShardingSphereDatabase database) {
        ShardingSphereSchema schema = database.getSchema(schemaName);
        ShardingSpherePreconditions.checkNotNull(schema, () -> new SchemaNotFoundException(schemaName));
        return schema;
    }

    // [Custom Modification]: remove getDatabaseName method, use getAndCheckDatabase method instead
    // private static IdentifierValue getDatabaseName(final SimpleTableSegment segment, final SQLStatementBinderContext binderContext) {
    //     DialectDatabaseMetaData dialectDatabaseMetaData = new DatabaseTypeRegistry(binderContext.getSqlStatement().getDatabaseType()).getDialectDatabaseMetaData();
    //     Optional<OwnerSegment> owner = dialectDatabaseMetaData.getDefaultSchema().isPresent() ? segment.getOwner().flatMap(OwnerSegment::getOwner) : segment.getOwner();
    //     IdentifierValue result = new IdentifierValue(owner.map(optional -> optional.getIdentifier().getValue()).orElse(binderContext.getCurrentDatabaseName()));
    //     ShardingSpherePreconditions.checkNotNull(result.getValue(), NoDatabaseSelectedException::new);
    //     ShardingSpherePreconditions.checkState(binderContext.getMetaData().containsDatabase(result.getValue()), () -> new UnknownDatabaseException(result.getValue()));
    //     return result;
    // }

    // [Custom Modification]: remove getSchemaName methods, use getSchemaAndCheckTableExists and getAndCheckSchema method instead
    // private static IdentifierValue getSchemaName(final SimpleTableSegment segment, final SQLStatementBinderContext binderContext, final IdentifierValue databaseName) {
    //     IdentifierValue result = getSchemaName(segment, binderContext);
    //     ShardingSpherePreconditions.checkState(binderContext.getMetaData().getDatabase(databaseName.getValue()).containsSchema(result.getValue()),
    //             () -> new SchemaNotFoundException(result.getValue()));
    //     return result;
    // }
    //
    // private static IdentifierValue getSchemaName(final SimpleTableSegment segment, final SQLStatementBinderContext binderContext) {
    //     if (segment.getOwner().isPresent()) {
    //         return segment.getOwner().get().getIdentifier();
    //     }
    //     // TODO getSchemaName according to search path
    //     DatabaseType databaseType = binderContext.getSqlStatement().getDatabaseType();
    //     if ((databaseType instanceof PostgreSQLDatabaseType || databaseType instanceof OpenGaussDatabaseType)
    //             && SystemSchemaManager.isSystemTable(databaseType.getType(), PG_CATALOG, segment.getTableName().getIdentifier().getValue())) {
    //         return new IdentifierValue(PG_CATALOG);
    //     }
    //     return new IdentifierValue(new DatabaseTypeRegistry(databaseType).getDefaultSchemaName(binderContext.getCurrentDatabaseName()));
    // }

    // [Custom Modification]: remove schemaName parameter
    private static void checkTableExists(final SQLStatementBinderContext binderContext, final ShardingSphereSchema schema, /*final String schemaName, */final String tableName) {
        // TODO refactor table exists check with spi @duanzhengqiang
        if (binderContext.getSqlStatement() instanceof CreateTableStatement && isCreateTable(((CreateTableStatement) binderContext.getSqlStatement()).getTable(), tableName)) {
            ShardingSpherePreconditions.checkState(binderContext.getHintValueContext().isSkipMetadataValidate()
                    || ((CreateTableStatement) binderContext.getSqlStatement()).isIfNotExists() || !schema.containsTable(tableName), () -> new TableExistsException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof AlterTableStatement && isRenameTable((AlterTableStatement) binderContext.getSqlStatement(), tableName)) {
            ShardingSpherePreconditions.checkState(binderContext.getHintValueContext().isSkipMetadataValidate() || !schema.containsTable(tableName), () -> new TableExistsException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof DropTableStatement) {
            ShardingSpherePreconditions.checkState(((DropTableStatement) binderContext.getSqlStatement()).isIfExists() || schema.containsTable(tableName), () -> new TableNotFoundException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof RenameTableStatement && isRenameTable((RenameTableStatement) binderContext.getSqlStatement(), tableName)) {
            ShardingSpherePreconditions.checkState(binderContext.getHintValueContext().isSkipMetadataValidate() || !schema.containsTable(tableName), () -> new TableExistsException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof CreateViewStatement && isCreateTable(((CreateViewStatement) binderContext.getSqlStatement()).getView(), tableName)) {
            ShardingSpherePreconditions.checkState(binderContext.getHintValueContext().isSkipMetadataValidate()
                    || ((CreateViewStatement) binderContext.getSqlStatement()).isReplaceView() || !schema.containsTable(tableName), () -> new TableExistsException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof AlterViewStatement && isRenameView((AlterViewStatement) binderContext.getSqlStatement(), tableName)) {
            ShardingSpherePreconditions.checkState(binderContext.getHintValueContext().isSkipMetadataValidate() || !schema.containsTable(tableName), () -> new TableExistsException(tableName));
            return;
        }
        if (binderContext.getSqlStatement() instanceof DropViewStatement) {
            ShardingSpherePreconditions.checkState(((DropViewStatement) binderContext.getSqlStatement()).isIfExists() || schema.containsTable(tableName), () -> new TableNotFoundException(tableName));
            return;
        }
        if ("DUAL".equalsIgnoreCase(tableName)) {
            return;
        }
        // if (SystemSchemaManager.isSystemTable(schemaName, tableName)) {
        //     return;
        // }
        if (binderContext.getExternalTableBinderContexts().containsKey(new CaseInsensitiveString(tableName))) {
            return;
        }
        if (binderContext.getCommonTableExpressionsSegmentsUniqueAliases().contains(tableName)) {
            return;
        }
        ShardingSpherePreconditions.checkState(schema.containsTable(tableName), () -> new TableNotFoundException(tableName));
    }
    
    private static boolean isCreateTable(final SimpleTableSegment simpleTableSegment, final String tableName) {
        return simpleTableSegment.getTableName().getIdentifier().getValue().equalsIgnoreCase(tableName);
    }
    
    private static boolean isRenameTable(final AlterTableStatement alterTableStatement, final String tableName) {
        return alterTableStatement.getRenameTable().isPresent() && alterTableStatement.getRenameTable().get().getTableName().getIdentifier().getValue().equalsIgnoreCase(tableName);
    }
    
    private static boolean isRenameTable(final RenameTableStatement renameTableStatement, final String tableName) {
        for (RenameTableDefinitionSegment each : renameTableStatement.getRenameTables()) {
            if (each.getRenameTable().getTableName().getIdentifier().getValue().equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }
    
    private static boolean isRenameView(final AlterViewStatement alterViewStatement, final String tableName) {
        return alterViewStatement.getRenameView().isPresent() && alterViewStatement.getRenameView().get().getTableName().getIdentifier().getValue().equalsIgnoreCase(tableName);
    }

    // [Custom Modification]: Change the parameters of the following methods
    private static SimpleTableSegmentBinderContext createSimpleTableBinderContext(final SimpleTableSegment segment,
                                                                                  final DatabaseTypeRegistry registry,
                                                                                  final ShardingSphereSchema schema,
                                                                                  final IdentifierValue tableName,
                                                                                  final TableSegmentBoundInfo boundInfo,
                                                                                  final SQLStatementBinderContext binderContext) {
        if (schema.containsTable(tableName.getValue())) {
            return createSimpleTableSegmentBinderContextWithMetaData(segment, registry, schema, tableName, boundInfo);
        }
        if (binderContext.getSqlStatement() instanceof CreateTableStatement) {
            return new SimpleTableSegmentBinderContext(createProjectionSegments((CreateTableStatement) binderContext.getSqlStatement(), tableName, boundInfo));
        }
        CaseInsensitiveString caseInsensitiveTableName = new CaseInsensitiveString(tableName.getValue());
        if (binderContext.getExternalTableBinderContexts().containsKey(caseInsensitiveTableName)) {
            TableSegmentBinderContext tableSegmentBinderContext = binderContext.getExternalTableBinderContexts().get(caseInsensitiveTableName).iterator().next();
            return new SimpleTableSegmentBinderContext(
                    SubqueryTableBindUtils.createSubqueryProjections(tableSegmentBinderContext.getProjectionSegments(), tableName, binderContext.getSqlStatement().getDatabaseType()));
        }
        return new SimpleTableSegmentBinderContext(Collections.emptyList());
    }
    
    private static Collection<ProjectionSegment> createProjectionSegments(final CreateTableStatement sqlStatement,
                                                                          final IdentifierValue tableName,
                                                                          final TableSegmentBoundInfo boundInfo) {
        Collection<ProjectionSegment> result = new LinkedList<>();
        for (ColumnDefinitionSegment each : sqlStatement.getColumnDefinitions()) {
            each.getColumnName().setColumnBoundInfo(new ColumnSegmentBoundInfo(boundInfo, tableName, each.getColumnName().getIdentifier()));
            result.add(new ColumnProjectionSegment(each.getColumnName()));
        }
        return result;
    }
    
    private static SimpleTableSegmentBinderContext createSimpleTableSegmentBinderContextWithMetaData(final SimpleTableSegment segment,
                                                                                                     final DatabaseTypeRegistry registry,
                                                                                                     final ShardingSphereSchema schema,
                                                                                                     final IdentifierValue tableName,
                                                                                                     final TableSegmentBoundInfo boundInfo) {
        Collection<ProjectionSegment> projectionSegments = new LinkedList<>();
        QuoteCharacter quoteCharacter = registry.getDialectDatabaseMetaData().getQuoteCharacter();
        for (ShardingSphereColumn each : schema.getTable(tableName.getValue()).getAllColumns()) {
            IdentifierValue columnName = new IdentifierValue(each.getName(), quoteCharacter);
            ColumnProjectionSegment columnProjectionSegment = new ColumnProjectionSegment(createColumnSegment(segment, columnName, tableName, boundInfo));
            columnProjectionSegment.setVisible(each.isVisible());
            projectionSegments.add(columnProjectionSegment);
        }
        return new SimpleTableSegmentBinderContext(projectionSegments);
    }
    
    private static ColumnSegment createColumnSegment(final SimpleTableSegment segment,
                                                     final IdentifierValue columnName,
                                                     final IdentifierValue tableName,
                                                     final TableSegmentBoundInfo boundInfo) {
        ColumnSegment result = new ColumnSegment(0, 0, columnName);
        result.setOwner(new OwnerSegment(0, 0, segment.getAlias().orElse(tableName)));
        result.setColumnBoundInfo(new ColumnSegmentBoundInfo(boundInfo, tableName, columnName));
        return result;
    }
}
