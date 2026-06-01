package io.flatf.foundation.persistence.h2;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Before;

import javax.sql.DataSource;

/**
 * Created by sky on 16-1-18.
 */
public class H2TestBase {

    protected DataSource dataSource;

    @Before
    public void prepareH2() {
        JdbcDataSource ds = new JdbcDataSource();
        // choose file system or memory to save data files
        ds.setURL(
                "jdbc:h2:~/test;MODE=POSTGRESQL;INIT=runscript from './src/test/resources/createTable.sql'");
        ds.setURL(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=POSTGRESQL;INIT=runscript from './src/test/resources/createTable.sql'");
        ds.setURL(
                "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=POSTGRESQL;INIT=runscript from './src/test/resources/createTable.sql'");

        ds.setUser("sa");
        ds.setPassword("");

        dataSource = ds;
    }
}
