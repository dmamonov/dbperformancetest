import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * @author dmitry.mamonov
 *         Created: 2014-08-13 11:24 PM
 */
public class Main {
    private static final String host = System.getProperty("host", "localhost");

    public static void main(final String[] args) throws InterruptedException, IOException {
        final int[] sizes = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20, 24, 28, 32, 48, 64, 96, 128, 192, 256};
        for(final int size:sizes) {
            for(final DatabaseType databaseType: new DatabaseType[]{DatabaseType.postgres, DatabaseType.mysql, DatabaseType.mongo}) {
                runTestSuite(DatabaseType.mongo, new int[]{size}, true);
            }
        }
        System.out.println("All Done");
    }

    private static void runTestSuite(final DatabaseType databaseType, final int[] sizes, final boolean runAllTests) throws InterruptedException, IOException {
        for (final int maxPoolSize : sizes) { //test inserts:
            if (runAllTests || true) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setWriteInsertThreads(maxPoolSize)
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test update tiny:
            if (runAllTests || true) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setWriteUpdateTinyThreads(maxPoolSize)
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test update wide:
            if (runAllTests || true) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setWriteUpdateWideThreads(maxPoolSize)
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test select lite:
            if (runAllTests) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setReadLiteCpuThreads(maxPoolSize)
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test select heavy:
            if (runAllTests) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setReadHeavyCpuThreads(maxPoolSize)
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test select tiny, insert:
            if (runAllTests) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setReadLiteCpuThreads(Math.max(1, maxPoolSize / 2))
                                .setWriteInsertThreads(Math.max(1, maxPoolSize / 2))
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test select tiny, update tiny:
            if (runAllTests) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setReadLiteCpuThreads(Math.max(1, maxPoolSize / 2))
                                .setWriteUpdateTinyThreads(Math.max(1, maxPoolSize / 2))
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test select heavy, update wide:
            if (runAllTests) {
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setReadHeavyCpuThreads(Math.max(1, maxPoolSize / 2))
                                .setWriteUpdateWideThreads(Math.max(1, maxPoolSize / 2))
                                .build()
                );
            }
        }

        for (final int maxPoolSize : sizes) { //test insert, update, select:
            if (runAllTests) {
                final int writePoolSize = Math.max(1, maxPoolSize / 2);
                runTest(new TestConfig.Builder()
                                .setDatabaseType(databaseType)
                                .setSharedPoolSize(maxPoolSize)
                                .setWritePoolSize(writePoolSize)
                                .setReadLiteCpuThreads(maxPoolSize)
                                .setWriteInsertThreads(Math.max(1, writePoolSize / 3))
                                .setWriteUpdateTinyThreads(Math.max(1, writePoolSize / 3))
                                .setWriteUpdateTinyThreads(Math.max(1, writePoolSize / 3))
                                .build()
                );
            }
        }
    }

    static boolean javaWarmed = false;

    protected static enum DatabaseType {
        postgres("postgres", "postgres"),
        mysql("root", "123"),
        mongo("mongo", "mongo"),
        redis("redis", "redis");
        private final String user;
        private final String password;

        DatabaseType(final String user, final String password) {
            this.user = user;
            this.password = password;
        }

        public String getUser() {
            return user;
        }

        public String getPassword() {
            return password;
        }
    }

    private static class TestConfig {
        private DatabaseType databaseType = DatabaseType.postgres;
        private int sharedPoolSize;
        private int writePoolSize = 0;
        private int readLiteCpuThreads = 0;
        private int readHeavyCpuThreads = 0;
        private int writeInsertThreads = 0;
        private int writeUpdateTinyThreads = 0;
        private int writeUpdateWideThreads = 0;
        private int readRange = 100_000;
        private int readSize = 1000;
        private int updateRange = 400_000;

        public DatabaseType getDatabaseType() {
            return databaseType;
        }

        public int getSharedPoolSize() {
            return sharedPoolSize;
        }

        public int getWritePoolSize() {
            return writePoolSize;
        }

        public int getReadLiteCpuThreads() {
            return readLiteCpuThreads;
        }

        public int getReadHeavyCpuThreads() {
            return readHeavyCpuThreads;
        }

        public int getWriteInsertThreads() {
            return writeInsertThreads;
        }

        public int getWriteUpdateTinyThreads() {
            return writeUpdateTinyThreads;
        }

        public int getWriteUpdateWideThreads() {
            return writeUpdateWideThreads;
        }

        public int getReadRange() {
            return readRange;
        }

        public int getReadSize() {
            return readSize;
        }

        public int getUpdateRange() {
            return updateRange;
        }

        @Override
        public String toString() {
            return Joiner.on(",").join(ImmutableList.of(
                    String.format("sp=%04d", sharedPoolSize),
                    String.format("wp=%04d", writePoolSize),
                    String.format("r_lite=%04d", readLiteCpuThreads),
                    String.format("r_heavy=%04d", readHeavyCpuThreads),
                    String.format("w_ins=%04d", writeInsertThreads),
                    String.format("w_up_tiny=%04d", writeUpdateTinyThreads),
                    String.format("w_up_wide=%04d", writeUpdateWideThreads)
            ));
        }

        protected static class Builder {
            private TestConfig delegate = new TestConfig();

            public Builder setDatabaseType(final DatabaseType databaseType) {
                delegate.databaseType = databaseType;
                return this;
            }

            public Builder setSharedPoolSize(final int sharedPoolSize) {
                delegate.sharedPoolSize = sharedPoolSize;
                return this;
            }

            public Builder setWritePoolSize(final int writePoolSize) {
                delegate.writePoolSize = writePoolSize;
                return this;
            }

            public Builder setReadLiteCpuThreads(final int readLiteCpuThreads) {
                delegate.readLiteCpuThreads = readLiteCpuThreads;
                return this;
            }

            public Builder setReadHeavyCpuThreads(final int readHeavyCpuThreads) {
                delegate.readHeavyCpuThreads = readHeavyCpuThreads;
                return this;
            }

            public Builder setWriteInsertThreads(final int writeInsertThreads) {
                delegate.writeInsertThreads = writeInsertThreads;
                return this;
            }

            public Builder setWriteUpdateTinyThreads(final int writeUpdateTinyThreads) {
                delegate.writeUpdateTinyThreads = writeUpdateTinyThreads;
                return this;
            }

            public Builder setWriteUpdateWideThreads(final int writeUpdateWideThreads) {
                delegate.writeUpdateWideThreads = writeUpdateWideThreads;
                return this;
            }

            public TestConfig build() {
                final TestConfig result = checkNotNull(this.delegate, "Builder closed");
                this.delegate = null; //close builder.
                return result;
            }
        }
    }

    private static void runTest(final TestConfig config) throws InterruptedException, IOException {
        if (!javaWarmed){
            Thread.sleep(5000);
        }
        try {
            runTestImpl(config);
        } catch (final Exception oops) {
            oops.printStackTrace();
        }
    }


    private static void runTestImpl(final TestConfig config) throws InterruptedException, IOException {
        final String testName = config.toString();
        final File csvFile = new File(String.format("data/%s/%s/%s.csv", config.getDatabaseType(), host, config.toString()));
        if (csvFile.exists()) {
            return;
        }


        final DatabaseTestQueries sharedTestQueries = createTestQueries(config.getDatabaseType(), config.getSharedPoolSize());
        final DatabaseTestQueries writeTestQueries = config.getWritePoolSize() > 0
                ? createTestQueries(config.getDatabaseType(), config.getWritePoolSize())
                : sharedTestQueries;

        sharedTestQueries.init();


        final AtomicInteger readProgress = new AtomicInteger();
        final AtomicInteger readFailures = new AtomicInteger();
        final AtomicInteger writeProgress = new AtomicInteger();
        final AtomicInteger writeFailures = new AtomicInteger();

        final AtomicBoolean stop = new AtomicBoolean(false);

        abstract class AbstractLoadThread extends Thread {
            protected final AtomicInteger successCounter;
            protected final AtomicInteger failuresCounter;

            AbstractLoadThread(final AtomicInteger successCounter, final AtomicInteger failuresCounter) {
                this.successCounter = successCounter;
                this.failuresCounter = failuresCounter;
            }

            @Override
            public void run() {
                while (!stop.get()) {
                    if (perform()) {
                        successCounter.incrementAndGet();
                    } else {
                        failuresCounter.incrementAndGet();
                    }
                }
            }

            abstract boolean perform();
        }

        abstract class AbstractReadLoadThread extends AbstractLoadThread {
            protected AbstractReadLoadThread() {
                super(readProgress, readFailures);
            }
        }

        abstract class AbstractWriteLoadThread extends AbstractLoadThread {
            protected AbstractWriteLoadThread() {
                super(writeProgress, writeFailures);
            }
        }

        for (int i = 0; i < config.getReadLiteCpuThreads(); i++) {
            new AbstractReadLoadThread() {
                @Override
                boolean perform() {
                    return sharedTestQueries.selectCpuLite(1, config.getReadRange());
                }
            }.start();
        }

        for (int i = 0; i < config.getReadLiteCpuThreads(); i++) {
            new AbstractReadLoadThread() {
                @Override
                boolean perform() {
                    return sharedTestQueries.selectCpuLite(1, config.getReadRange());
                }
            }.start();
        }

        for (int i = 0; i < config.getReadHeavyCpuThreads(); i++) {
            new AbstractReadLoadThread() {
                @Override
                boolean perform() {
                    return sharedTestQueries.selectCpuHeavy(1, config.getReadRange(), config.getReadSize());
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteInsertThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.insert();
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteUpdateTinyThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.updateTinyColumnById(1, config.getUpdateRange());
                }
            }.start();
        }

        for (int i = 0; i < config.getWriteUpdateWideThreads(); i++) {
            new AbstractWriteLoadThread() {
                @Override
                boolean perform() {
                    return writeTestQueries.updateWideColumnById(1, config.getUpdateRange());
                }
            }.start();
        }


        if (javaWarmed) {
            Thread.sleep(15000);
        } else {
            System.out.println("Warm JVM");
            Thread.sleep(40000);
            javaWarmed = true;
        }

        final StringBuilder csv = new StringBuilder("'Time','ReadOps','ReadErr','WriteOps','WriteErr'\n");

        //start tracking:
        System.out.println("Start tracking ("+config.getDatabaseType()+"): " + testName);
        for (int time = 0; time < 60 * 5 / 5; time++) {
            if (time % 10 == 0) {
                //cleanup environment:
                System.gc();
                for (final AtomicInteger reset : new AtomicInteger[]{readProgress, readFailures, writeProgress, writeFailures}) {
                    reset.set(0);
                }
            }
            final long secondStart = System.currentTimeMillis();
            Thread.sleep(1000);
            final double duration = (System.currentTimeMillis() - secondStart) / 1000.0;


            final int readOpsSnapshot = readProgress.getAndSet(0);
            final int readErrorsSnapshot = readFailures.getAndSet(0);
            final int writeOpsSnapshot = writeProgress.getAndSet(0);
            final int writeErrorsSnapshot = writeFailures.getAndSet(0);
            System.out.printf("T %4d, R %5d/%5d, W %5d/%5d, dur=%.2f, Pools(%d/%d)\n",
                    time,
                    readOpsSnapshot, readErrorsSnapshot,
                    writeOpsSnapshot, writeErrorsSnapshot,
                    duration,
                    config.getSharedPoolSize(), config.getWritePoolSize());
            csv.append(String.format("%d,%d,%d,%d,%d\n", time, readOpsSnapshot, readErrorsSnapshot, writeOpsSnapshot, writeErrorsSnapshot));
        }

        if (!csvFile.getParentFile().exists()) {
            checkState(csvFile.getParentFile().mkdirs());
        }
        Files.write(csvFile.toPath(), csv.toString().getBytes(Charsets.UTF_8));
        System.out.println("Done");

        //cleanup after test
        stop.set(true);
        Thread.sleep(500);

        sharedTestQueries.close();
        if (writeTestQueries != sharedTestQueries) {
            writeTestQueries.close();
        }
    }

    private interface DatabaseTestQueries {
        final Random random = new Random();

        void init();

        boolean insert();

        boolean updateTinyColumnById(int rangeFrom, int rangeTo);

        boolean updateWideColumnById(int rangeFrom, int rangeTo);

        boolean selectCpuLite(int rangeFrom, int rangeTo);

        boolean selectCpuHeavy(int rangeFrom, int rangeTo, int size);

        void close();

        class JdbcTemplateTestQueries implements DatabaseTestQueries {
            private final HikariDataSource dataSource;
            private final JdbcTemplate sql;

            public JdbcTemplateTestQueries(final HikariDataSource dataSource) {
                this.dataSource = dataSource;
                this.sql = new JdbcTemplate(dataSource);
            }

            @Override
            public void init() {
                if (0 < sql.update("" +
                        "CREATE TABLE IF NOT EXISTS hikari (\n" +
                        "  id SERIAL NOT NULL PRIMARY KEY,\n" +
                        "  title TEXT NULL,\n" +
                        "  val INTEGER NOT NULL DEFAULT 0\n" +
                        ");")) {
                    sql.update("CREATE INDEX i_hikari_title ON hikari(title);");
                    sql.update("CREATE INDEX i_hikari_val ON hikari(val);");
                }
            }

            @Override
            public boolean insert() {
                try {
                    sql.update("INSERT INTO hikari (title, val) VALUES (repeat(?, 64), ?);", String.format("%08d", random.nextInt(100_000_000)), random.nextInt(1_000_000_000));
                    return true;
                } catch (final RuntimeException re) {
                    return false;
                }
            }

            @Override
            public boolean updateTinyColumnById(final int rangeFrom, final int rangeTo) {
                try {
                    final int idForTinyUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
                    sql.update("UPDATE hikari SET val=? WHERE id=?;", random.nextInt(1_000_000_000), idForTinyUpdate);
                    return true;
                } catch (final RuntimeException re) {
                    return false;
                }
            }

            @Override
            public boolean updateWideColumnById(final int rangeFrom, final int rangeTo) {
                try {
                    sql.update("UPDATE hikari SET title=repeat(?, 512) WHERE id=?;", String.valueOf(random.nextInt(10)), rangeFrom + (random.nextInt(rangeTo - rangeFrom)));
                    return true;
                } catch (final RuntimeException re) {
                    return false;
                }
            }

            @Override
            public boolean selectCpuLite(final int rangeFrom, final int rangeTo) {
                try {
                    sql.queryForObject("SELECT val FROM hikari WHERE id=?", Integer.class, rangeFrom + (random.nextInt(rangeTo - rangeFrom)));
                    return true;
                } catch (final RuntimeException re) {
                    return false;
                }
            }

            @Override
            public boolean selectCpuHeavy(final int rangeFrom, final int rangeTo, final int size) {
                try {
                    final int start = rangeFrom + random.nextInt(rangeTo - rangeFrom - size);
                    final int end = start + size;
                    sql.queryForObject("SELECT avg(val) FROM hikari WHERE id BETWEEN ? AND ?", Integer.class, start, end);
                    return true;
                } catch (final RuntimeException re) {
                    return false;
                }
            }

            @Override
            public void close() {
                this.dataSource.close();
            }
        }
    }

    private static DatabaseTestQueries createTestQueries(final DatabaseType databaseType, final int poolSize) {
        return new Callable<DatabaseTestQueries>() {
            @Override
            public DatabaseTestQueries call() {
                switch (databaseType) {
                    case postgres:
                        return createPostgresTestQueries();
                    case mysql:
                        return createMySqlTestQueries();
                    case mongo:
                        return createMongoTestQueries();
                    case redis:
                        return createRedisTestQueries();
                    default:
                        throw new IllegalArgumentException("Unknown database type: " + databaseType);
                }
            }

            private DatabaseTestQueries createPostgresTestQueries() {
                final HikariConfig config = createDefaultPoolConfig(poolSize);
                config.setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

                return new DatabaseTestQueries.JdbcTemplateTestQueries(new HikariDataSource(config));
            }

            private DatabaseTestQueries createMySqlTestQueries() {
                final HikariConfig config = createDefaultPoolConfig(poolSize);
                MysqlDataSource ds;
                config.setDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
                config.addDataSourceProperty("port", "3306");
                config.addDataSourceProperty("user", databaseType.getUser());
                config.addDataSourceProperty("password", databaseType.getPassword());

                return new DatabaseTestQueries.JdbcTemplateTestQueries(new HikariDataSource(config));
            }

            private DatabaseTestQueries createMongoTestQueries() {
                try {
                    final MongoClient client = new MongoClient(host, new MongoClientOptions.Builder()
                            .connectionsPerHost(poolSize)
                            .build());
                    final MongoTemplate mongo = new MongoTemplate(
                            new SimpleMongoDbFactory(
                                    client,
                                    "demo"));
                    return new DatabaseTestQueries() {
                        @Override
                        public void init() {
                            try {
                                mongo.createCollection("hikari");
                                persistentId.reset();
                            } catch (final RuntimeException oops){
                                if (!oops.getMessage().contains("exists")){
                                    throw oops;
                                }
                            }
                            try {
                                mongo.insert(new MongoDomain(), "hikari");
                            } catch (final DuplicateKeyException ignore){
                                //ignore.
                            }
                            mongo.indexOps("hikari").ensureIndex(new Index().on("id", Sort.Direction.ASC));
                            mongo.indexOps("hikari").ensureIndex(new Index().on("title", Sort.Direction.ASC));
                            mongo.indexOps("hikari").ensureIndex(new Index().on("val", Sort.Direction.ASC));
                        }

                        @Override
                        public boolean insert() {
                            try {
                                mongo.insert(new MongoDomain(), "hikari");
                                return true;
                            } catch (final RuntimeException re) {
                                //System.out.println(re.getMessage());
                                return false;
                            }
                        }

                        @Override
                        public boolean updateTinyColumnById(final int rangeFrom, final int rangeTo) {
                            try {
                                final int idForTinyUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
                                mongo.updateFirst(new Query(Criteria.where("id").is(idForTinyUpdate)), new Update().set("val", random.nextInt()), "hikari");
                                return true;
                            } catch (final RuntimeException re) {
                                return false;
                            }
                        }

                        @Override
                        public boolean updateWideColumnById(final int rangeFrom, final int rangeTo) {
                            try {
                                final int idForWideUpdate = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
                                final StringBuilder titleBuilder = new StringBuilder();
                                {
                                    final String titlePart = String.format("%08d", DatabaseTestQueries.random.nextInt(100_000_000));
                                    for (int i = 0; i < 16; i++) {
                                        titleBuilder.append(titlePart);
                                    }
                                }
                                mongo.updateFirst(new Query(Criteria.where("id").is(idForWideUpdate)), new Update().set("title", titleBuilder.toString()), "hikari");
                                return true;
                            } catch (final RuntimeException re) {
                                return false;
                            }
                        }

                        @Override
                        public boolean selectCpuLite(final int rangeFrom, final int rangeTo) {
                            try {
                                final int idToFind = rangeFrom + (random.nextInt(rangeTo - rangeFrom));
                                mongo.find(new Query(Criteria.where("id").is(idToFind)), MongoDomain.class);
                                return true;
                            } catch (final RuntimeException re) {
                                return false;
                            }
                        }

                        @Override
                        public boolean selectCpuHeavy(final int rangeFrom, final int rangeTo, final int size) {
                            try {
                                final int start = rangeFrom + random.nextInt(rangeTo - rangeFrom - size);
                                final int end = start + size;
                                mongo.aggregate(Aggregation.newAggregation(
                                                Aggregation.match(new Criteria("id").lte(end).gte(start)),
                                                Aggregation.group("val").sum("val").as("val")
                                        ),
                                        "hikari", MongoDomain.class);
                                return true;
                            } catch (final RuntimeException re) {
                                return false;
                            }
                        }

                        @Override
                        public void close() {
                            client.close();
                        }
                    };
                } catch (final UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }

            private DatabaseTestQueries createRedisTestQueries() {
                return null;
            }

            private HikariConfig createDefaultPoolConfig(final int poolSize) {
                final HikariConfig config = new HikariConfig();
                config.setAutoCommit(true);
                config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(1));
                config.setIdleTimeout(MINUTES.toMillis(5));
                config.setMaxLifetime(HOURS.toMillis(1));
                config.setLeakDetectionThreshold(0);
                config.setInitializationFailFast(false);
                config.setJdbc4ConnectionTest(true);
                //config.setConnectionInitSql("SELECT 1");
                config.setMaximumPoolSize(poolSize);
                config.setIsolateInternalQueries(false);
                config.setRegisterMbeans(false);
                config.setUsername(databaseType.getUser());
                config.setPassword(databaseType.getPassword());

                config.addDataSourceProperty("databaseName", "demo");
                config.setPoolName("demo-ds");
                config.addDataSourceProperty("serverName", host);

                return config;
            }
        }.call();
    }

    public static class MongoDomain {
        public long id;
        public String title;
        public int val;

        public MongoDomain() {
            this.id=persistentId.nextId();
            final StringBuilder titleBuilder = new StringBuilder();
            {
                final String titlePart = String.format("%08d", DatabaseTestQueries.random.nextInt(100_000_000));
                for (int i = 0; i < 16; i++) {
                    titleBuilder.append(titlePart);
                }
            }
            this.title = titleBuilder.toString();
            this.val = DatabaseTestQueries.random.nextInt();

        }
    }


    private static final PersistentId persistentId = new PersistentId();
    private static final class PersistentId {
        private static final File idFile = new File("id.seq");
        private final AtomicInteger idSeq = new AtomicInteger(0);

        PersistentId() {
            if (idFile.exists()){
                try {
                    idSeq.set(Integer.parseInt(new String(Files.readAllBytes(idFile.toPath())).trim()));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public int nextId(){
            final int result = idSeq.incrementAndGet();
            if (result%1000==0) {
                try {
                    Files.write(idFile.toPath(), String.valueOf(result).getBytes(Charsets.UTF_8));
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        public void reset() {
            checkState(idFile.delete(), "Failed to delete id file: %s", idFile);
        }
    }
}

