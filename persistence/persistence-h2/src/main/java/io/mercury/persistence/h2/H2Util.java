package io.mercury.persistence.h2;

import io.mercury.common.lang.Validator;
import io.mercury.common.log4j2.Log4j2LoggerFactory;
import io.mercury.common.sys.SysProperties;
import io.mercury.common.util.StringSupport;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static io.mercury.common.file.FileUtil.mkdirInHome;

public final class H2Util {

    private static final Logger log = Log4j2LoggerFactory.getLogger(H2Util.class);

    public static final String JdbcProtocol = "jdbc:sqlite:";

    private H2Util() {
    }

    /**
     * @param dir    String
     * @param dbName String
     * @return String
     */
    public static String getH2UrlInHome(@Nonnull String dir, @Nonnull String dbName) {
        if (!dbName.endsWith(".db"))
            dbName = dbName + ".db";
        mkdirInHome(dir);
        return JdbcProtocol + StringSupport.fixPath(SysProperties.USER_HOME) + StringSupport.fixPath(dir) + dbName;
    }

    /**
     * @param connection Connection
     * @param sql        String
     * @param processor  ResultSetProcessor
     * @param type       Class<T>
     * @return List<T>
     * @throws SQLException e
     */
    public static <T> List<T> query(@Nonnull Connection connection,
                                    @Nonnull String sql,
                                    @Nonnull ResultSetProcessor processor,
                                    @Nonnull Class<T> type) throws SQLException {
        Validator.nonNull(connection, "connection");
        try (// create a database connection
             Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            return processor.toBeanList(rs, type);
        } catch (SQLException e) {
            log.error("error message -> {}", e.getMessage(), e);
            throw e;
        }
    }

    public static void main(String[] args) {
        System.out.println(H2Util.getH2UrlInHome("h2-file", "cyg_db"));
    }

}
