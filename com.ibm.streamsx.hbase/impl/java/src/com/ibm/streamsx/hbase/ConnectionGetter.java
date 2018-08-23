package com.ibm.streamsx.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class ConnectionGetter {
    private Configuration configuration;

    public ConnectionGetter(Configuration configuration) {
        this.configuration = configuration;
    }

    public Connection getConnection() throws IOException {
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create(configuration));
        return connection;
    }
}
