package com.dremio.exec.store.jdbc.conf;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.exec.store.jdbc.JdbcSchemaFetcher;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.exec.store.jdbc.dialect.arp.ArpYaml;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

public class OmnisciDialect extends ArpDialect {

    public OmnisciDialect(ArpYaml yaml) {
        super(yaml);
    }

    @Override
    public ContainerSupport supportsCatalogs() {
        return ContainerSupport.AUTO_DETECT;
    }

    @Override
    public ContainerSupport supportsSchemas() {
        return ContainerSupport.AUTO_DETECT;
    }

    @Override
    public JdbcSchemaFetcher getSchemaFetcher(String name, DataSource dataSource, int timeout, JdbcStoragePlugin.Config config) {
        return new OmnisciSchemaFetcher(name,dataSource,timeout,config);
    }

    @Override
    public boolean supportsNestedAggregations() {
        return false;
    }
}
