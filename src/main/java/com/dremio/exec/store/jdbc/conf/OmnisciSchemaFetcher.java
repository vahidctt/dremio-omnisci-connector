package com.dremio.exec.store.jdbc.conf;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.*;
import java.util.*;

class OmnisciSchemaFetcher extends JdbcSchemaFetcher {


    private final DataSource mDataSource;
    private final String mStoragePluginName;
    private final JdbcStoragePlugin.Config mConfig;
    private final int mTimeout;
    private final String mName;

    public OmnisciSchemaFetcher(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {
        super(name, dataSource, timeout, config);
        this.mDataSource = dataSource;
        this.mName=name;
        this.mStoragePluginName = name;
        this.mTimeout = timeout;
        this.mConfig = config;
    }

    @Override
    protected long getRowCount(JdbcDatasetHandle handle) {
        Optional<Long> count = this.executeQueryAndGetFirstLong("select count(*) from " + this.getQuotedPath(handle.getIdentifiers()));
        if (count.isPresent()) {
            return (Long)count.get();
        } else {
            return 1000000000L;
        }
    }

    @Override
    protected boolean usePrepareForColumnMetadata() {
        return true;
    }

    @Override
    protected boolean usePrepareForGetTables() {
        return false;
    }

    @Override
    public DatasetMetadata getTableMetadata(DatasetHandle datasetHandle) {
        return super.getTableMetadata(datasetHandle);
    }

    @Override
    public Optional<DatasetHandle> getTableHandle(List<String> tableSchemaPath) {
        try
        {
            DatabaseMetaData metaData = mDataSource.getConnection().getMetaData();
            List<String> trimmedList = tableSchemaPath.subList(1, tableSchemaPath.size());
            PreparedStatement statement = mDataSource.getConnection().prepareStatement("SELECT * FROM " + this.getQuotedPath(trimmedList));
            ResultSetMetaData preparedMetadata = statement.getMetaData();
            if (preparedMetadata.getColumnCount() > 0) {
                ImmutableList.Builder<String> pathBuilder = ImmutableList.builder();
                pathBuilder.add(this.storagePluginName);
                String table;
                if (supportsCatalogs(this.config.getDialect(), metaData)) {
                    table = preparedMetadata.getCatalogName(1);
                    if (!Strings.isNullOrEmpty(table)) {
                        pathBuilder.add(table);
                    }
                }

                if (supportsSchemas(this.config.getDialect(), metaData)) {
                    table = preparedMetadata.getSchemaName(1);
                    if (!Strings.isNullOrEmpty(table)) {
                        pathBuilder.add(table);
                    }
                }

                table = trimmedList.get(trimmedList.size()-1);//preparedMetadata.getTableName(1);


                pathBuilder.add(table);
                ImmutableList<String> tableName = pathBuilder.build();
                Optional var10 = Optional.of(new JdbcSchemaFetcher.JdbcDatasetHandle(new EntityPath(pathBuilder.build())));
                //Optional var10 = Optional.of(new OmnisciDatasetHandle(new EntityPath(tableName)));
                return var10;
            }

        }catch (Exception e)
        {

        }
        return Optional.empty();
    }

    @Override
    public DatasetHandleListing getTableHandles() {
        ArrayList<String> tableNames=new ArrayList<>();
        try {
            DatabaseMetaData metaData = mDataSource.getConnection().getMetaData();
            String[] types = {"TABLE","VIEW"};
            //Retrieving the columns in the database
            ResultSet tables = metaData.getTables(null, null, "%", types);
            while (tables.next()) {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return new DatasetHandleListing() {
            @Override
            public Iterator<? extends DatasetHandle> iterator() {
                return tableNames.stream().map(tableName -> {
                    final EntityPath entityPath = new EntityPath(ImmutableList.of(mName, tableName));
                    return new JdbcSchemaFetcher.JdbcDatasetHandle(entityPath);//new OmnisciDatasetHandle(entityPath);
                }).iterator();
            }

            @Override
            public void close() {
                try {
                    mDataSource.getConnection().close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}