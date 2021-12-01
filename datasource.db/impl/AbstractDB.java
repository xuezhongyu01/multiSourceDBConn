package com.wayz.lbi.industry.datasource.db.impl;

import com.wayz.lbi.industry.datasource.db.DB;

abstract class AbstractDB implements DB {

    /**
     * the name of the total statistic result
     */
    static final String TOTAL_FIELD = "total";

    /**
     * build the db url
     *
     * @param host   the host
     * @param port
     * @param dbName database name
     * @return
     */
    abstract String buildURL(String host, int port, String dbName);

    protected String buildKey(ConnectInfo info) {

        return buildURL(info.getHost(), info.getPort(), info.getDbName()) + ':' + info.getUserName()
            + ':' + info.getPassword();
    }
}
