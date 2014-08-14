import com.google.common.base.Charsets;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author dmitry.mamonov
 *         Created: 2014-08-13 11:24 PM
 */
public class Main {
    public static void main(final String[] args) throws InterruptedException, IOException {
        final int[] sizes = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 24, 28, 32, 48, 64, 96, 128, 192, 256};

        final boolean runAllTests = true;

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize, 0, maxPoolSize, false);
            }
        }

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize, maxPoolSize, 0, false);
            }
        }

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize*2, maxPoolSize, maxPoolSize, false);
            }
        }

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize, maxPoolSize, maxPoolSize, false);
            }
        }

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize, maxPoolSize, maxPoolSize, true);
            }
        }

        for (final int maxPoolSize : sizes) {
            if (runAllTests) {
                runTest(maxPoolSize, maxPoolSize, 1, true);
            }
        }
    }

    static boolean javaWarmed = false;

    static void runTest(final int maxPoolSize, final int readThreads, final int writeThreads, final boolean separateDataSources) throws InterruptedException, IOException {
        final String testName = String.format("Pool=%04d_Read=%04d_Write=%04d_Separated=%s", maxPoolSize, readThreads, writeThreads, separateDataSources);
        final File csvFile = new File("data/report/Test_" + testName + ".csv");
        if (csvFile.exists()) {
            return;
        }

        final HikariDataSource readDataSource = createDataSource(maxPoolSize);
        final HikariDataSource writeDataSource = separateDataSources?createDataSource(maxPoolSize):readDataSource;
        final JdbcTemplate readSql = new JdbcTemplate(readDataSource);
        final JdbcTemplate writeSql = new JdbcTemplate(writeDataSource);
        readSql.update("" +
                "CREATE TABLE IF NOT EXISTS hikari (\n" +
                "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                "  title TEXT NOT NULL DEFAULT '',\n" +
                "  val INTEGER NOT NULL DEFAULT 0\n" +
                ");");

        final AtomicInteger readProgress = new AtomicInteger();
        final AtomicInteger readFailures = new AtomicInteger();
        final AtomicInteger writeProgress = new AtomicInteger();
        final AtomicInteger writeFailures = new AtomicInteger();
        final Random random = new Random();

        final AtomicBoolean stop = new AtomicBoolean(false);
        for (int reader = 0; reader < readThreads; reader++) {
            new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        try {
                            if (false) {
                                final int max = readSql.queryForInt("SELECT max(id) from hikari");
                                final int range = 1000;
                                final int start = random.nextInt(Math.max(1, Math.min(max, 100000) - range));
                                readSql.queryForInt("SELECT sum(val) FROM hikari WHERE id BETWEEN ? AND ?", start, start + range);
                            } else {
                                readSql.queryForInt("SELECT val FROM hikari WHERE id=?", random.nextInt(10000));
                            }
                            readProgress.incrementAndGet();
                        } catch (final RuntimeException re) {
                            //re.printStackTrace();
                            readFailures.incrementAndGet();
                        }
                    }
                }
            }.start();
        }

        for (int reader = 0; reader < writeThreads; reader++) {
            new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        try {
                            writeSql.update("INSERT INTO hikari (title, val) VALUES (repeat('x', 512), ?);", random.nextInt(2000) - 1000);
                            writeProgress.incrementAndGet();
                        } catch (final RuntimeException re) {
                            //re.printStackTrace();
                            writeFailures.incrementAndGet();
                        }
                    }
                }
            }.start();
        }

        final StringBuilder csv = new StringBuilder(String.format("'Time','ReadOps_%d/%d/%d','ReadErr_%d/%d/%d','WriteOps_%d/%d/%d','WriteErr_%d/%d/%d'\n",
                readThreads, maxPoolSize, writeThreads,
                readThreads, maxPoolSize, writeThreads,
                writeThreads, maxPoolSize, readThreads,
                writeThreads, maxPoolSize, readThreads));
        if (javaWarmed) {
            Thread.sleep(15000);
        } else {
            System.out.println("Warm JVM");
            Thread.sleep(40000);
            javaWarmed = true;
        }
        System.gc();
        for (final AtomicInteger reset : new AtomicInteger[]{readProgress, readFailures, writeProgress, writeFailures}) {
            reset.set(0);
        }
        System.out.println("Start tracking");
        for (int time = 0; time < 60 * 5 / 5; time++) {
            final long secondStart = System.currentTimeMillis();
            Thread.sleep(1000);
            final double duration = (System.currentTimeMillis() - secondStart) / 1000.0;


            final int readOpsSnapshot = readProgress.getAndSet(0);
            final int readErrorsSnapshot = readFailures.getAndSet(0);
            final int writeOpsSnapshot = writeProgress.getAndSet(0);
            final int writeErrorsSnapshot = writeFailures.getAndSet(0);
            System.out.println(String.format("T %4d, P %02d, R %5d/%5d, W %5d/%5d, dur=%.2f",
                    time,
                    maxPoolSize,
                    readOpsSnapshot, readErrorsSnapshot,
                    writeOpsSnapshot, writeErrorsSnapshot,
                    duration));
            csv.append(String.format("%d,%d,%d,%d,%d\n", time, readOpsSnapshot, readErrorsSnapshot, writeOpsSnapshot, writeErrorsSnapshot));
        }

        if (!csvFile.getParentFile().exists()) {
            checkState(csvFile.getParentFile().mkdirs());
        }
        Files.write(csvFile.toPath(), csv.toString().getBytes(Charsets.UTF_8));
        System.out.println("Done");
        stop.set(true);
        Thread.sleep(500);
        readDataSource.close();
        if (writeDataSource != readDataSource) {
            writeDataSource.close();
        }
    }

    private static HikariDataSource createDataSource(final int maxPoolSize) {
        final HikariConfig config = new HikariConfig();
        config.setAutoCommit(true);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(1));
        config.setIdleTimeout(MINUTES.toMillis(1));
        config.setMaxLifetime(HOURS.toMillis(1));
        config.setLeakDetectionThreshold(0);
        config.setInitializationFailFast(false);
        config.setJdbc4ConnectionTest(true);
        config.setConnectionInitSql("SELECT 1");
        config.setMaximumPoolSize(maxPoolSize);
        config.setIsolateInternalQueries(false);
        config.setPoolName("demo-ds");
        config.setRegisterMbeans(true);
        config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");
        config.addDataSourceProperty("databaseName", "demo");
        config.addDataSourceProperty("serverName", System.getProperty("host","localhost"));
        config.setUsername("postgres");
        config.setPassword("postgres");
        return new HikariDataSource(config);
    }

}
