/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hibernate.validator.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin;
import com.dremio.exec.store.jdbc.JdbcStoragePlugin.Config;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.google.common.annotations.VisibleForTesting;

import io.protostuff.Tag;

import java.util.Properties;

@SourceType(value = "OMNISCI", label = "Omnisci")
public class OmnisciConf extends AbstractArpConf<OmnisciConf> {
    private static final String ARP_FILENAME = "arp/implementation/omnisci-arp.yaml";
    private static final ArpDialect ARP_DIALECT =
            AbstractArpConf.loadArpFile(ARP_FILENAME, (OmnisciDialect::new));
    private static final String DRIVER = "com.omnisci.jdbc.OmniSciDriver";


    @Tag(1)
    @DisplayMetadata(label = "Host")
    public String host;


    @Tag(2)
    @DisplayMetadata(label = "Port")
    public int port = 6274;


    @Tag(3)
    @DisplayMetadata(label = "Database")
    public String database;

    @Tag(4)
    @DisplayMetadata(label = "Username")
    public String username;

    @Tag(5)
    @DisplayMetadata(label = "Password")
    public String password; //default : HyperInteractive

    /*@Tag(6)
    @DisplayMetadata(label = "Record fetch size")
    @NotMetadataImpacting
    public int fetchSize = 200;*/

    @VisibleForTesting
    public String toJdbcConnectionString() {
        final String database = checkNotNull(this.database, "Missing database.");
        final String host = checkNotNull(this.host, "Missing host.");
        checkNotNull(this.username, "Missing username.");
        checkNotNull(this.password, "Missing password.");

        return String.format("jdbc:omnisci:%s:%d:%s", host,port,database);
    }

    @Override
    @VisibleForTesting
    public Config toPluginConfig(SabotContext context) {
        return JdbcStoragePlugin.Config.newBuilder()
                .withDialect(getDialect())
                //.withFetchSize(fetchSize)
                .withDatasourceFactory(this::newDataSource)
                .clearHiddenSchemas()
                .addHiddenSchema("SYSTEM")
                .build();
    }

    private CloseableDataSource newDataSource() {
        return DataSources.newGenericConnectionPoolDataSource(DRIVER,
                toJdbcConnectionString(), username, password, null, DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE);
    }

    @Override
    public ArpDialect getDialect() {
        return ARP_DIALECT;
    }

    @VisibleForTesting
    public static ArpDialect getDialectSingleton() {
        return ARP_DIALECT;
    }
}
