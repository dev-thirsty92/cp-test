package com.study.cp;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Slf4j
public class PerformanceTest {

    private static final String JDBC_URL =  "jdbc:h2:tcp://localhost/~/test";
    private static final String JDBC_USER = "sa";
    private static final String JDBC_PASSWORD = "";

    private static final int INSERT_COUNT = 5_000;

    @BeforeEach
    void setUp() {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(
                     "CREATE TABLE IF NOT EXISTS test (id INT AUTO_INCREMENT, value_text VARCHAR(100))"
             )
        ) {
            stmt.execute();
            log.info("[Start] --- Database setup complete.");
        } catch (SQLException e) {
            log.error(e.getMessage());
            Assertions.fail("[ERROR] --- Database setup failed, test cannot proceed.");
        }
    }


    @Test
    @DisplayName("다이렉트 커넥션을 이용한 데이터 인서트")
    @Order(1)
    void testDirectConnection() {
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < INSERT_COUNT; i++) {
                try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
                     PreparedStatement stmt = conn.prepareStatement("INSERT INTO test (value_text) VALUES (?)")) {
                    stmt.setString(1, "Direct Connection Test");
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        log.info("[DirectConnection] 수행시간: {}ms" , (System.currentTimeMillis() - startTime));
    }

    @Test
    @DisplayName("커넥션 풀을 이용한 데이터 인서트")
    @Order(2)
    void testConnectionPool(){
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(JDBC_USER);
        config.setPassword(JDBC_PASSWORD);
        config.setMaximumPoolSize(10); // 풀 크기는 10이지만 단일 스레드로 사용
        HikariDataSource dataSource = new HikariDataSource(config);

        long startTime = System.currentTimeMillis();

        try {
            for (int i = 0; i < INSERT_COUNT; i++) {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement stmt = conn.prepareStatement("INSERT INTO test (value_text) VALUES (?)")) {
                    stmt.setString(1, "Connection Pool Test");
                    stmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        dataSource.close();
        log.info("[ConnectionPool] 수행시간: {}ms" , (System.currentTimeMillis() - startTime));
    }

    @AfterEach
    void tearDown() {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
             PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS test")) {
            stmt.execute();
            log.info("[End] --- Database tear down complete.");
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
    }
}
