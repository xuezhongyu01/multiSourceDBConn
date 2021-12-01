package com.wayz.lbi.industry.datasource.db;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.wayz.lbi.industry.datasource.db.query.Condition;
import com.wayz.lbi.industry.datasource.db.query.Select;

import lombok.Data;
import lombok.experimental.Accessors;

public interface DB {

    @Data
    @Accessors(chain = true)
    class ConnectInfo {
        private String host;
        private int port;
        private String dbName;
        private String userName;
        private String password;

        public String displayString() {
            return userName + ":" + password + "@" + host + ":" + port + "/" + dbName;
        }
    }

    /**
     * connect to the db
     *
     * @param info connection info
     * @return true if success, false otherwise
     */
    boolean connect(ConnectInfo info);

    /**
     * list the tables of the db. it requires connecting db first
     *
     * @return table name list
     */
    List<String> showTables();

    /**
     * list the fields of the table. it requires connecting db first
     *
     * @param table cannot empty
     * @return table details. if the table is not exists, return empty
     */
    Optional<Table> descTable(String table);

    @Data
    @Accessors(chain = true)
    class FieldMap {
        private String dbFieldName;
        private String dataSourceFieldId;
    }

    /**
     * select with filter, group, sort
     *
     * @param select
     * @return
     */
    List<Map<String, Object>> select(Select select);

    /**
     * return the total row count
     *
     * @param table
     * @return
     */
    long total(String table, List<Condition> conditions);

    /**
     * return the query statement
     *
     * @param select select object
     * @return query statement
     */
    String queryStat(Select select);

    void batchInsert(String table, List<Map<String, Object>> data);
}
