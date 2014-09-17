package org.codefamily.crabs.jdbc.benchmark;

import org.codefamily.crabs.core.Identifier;
import org.codefamily.crabs.core.IndexDefinition;
import org.codefamily.crabs.core.TypeDefinition;
import org.codefamily.crabs.core.client.AdvancedClient;
import org.codefamily.crabs.core.client.AdvancedClient.ElasticsearchAddress;
import org.codefamily.crabs.core.client.AdvancedClient.InternalDocumentRequestBuilder;
import org.codefamily.crabs.core.client.AdvancedClient.ResponseCallback;
import org.codefamily.crabs.exception.SQL4ESException;
import org.codefamily.crabs.jdbc.Protocol;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.codefamily.crabs.jdbc.Protocol.PROPERTY_ENTRY$BENCHMARK_ENABLED;
import static org.codefamily.crabs.jdbc.Protocol.PROPERTY_ENTRY$OUTPUT_SQL_ES_MAPPING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Crabs 性能基准测试，重点关注相对于Elasticsearch原生API而言，Crabs在性能上的损耗。
 * <p/>
 * 主要测试两个指标：系统响应时间和吞吐量
 *
 * @author zhuchunlai
 * @version $Id: CrabsBenchmark.java, v1.0 2014/09/09 11:25 $
 */
public final class Benchmark {

    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

    private Benchmark() {
        // nothing to do.
    }

    public static void main(final String[] args) throws Exception {
        final String url = args[0];
        final int benchmarkType = Integer.parseInt(args[1]);
        if (benchmarkType == 0) {
            // 初始化
            final int documentCount = Integer.parseInt(args[2]);
            final int shardsNum = Integer.parseInt(args[3]);
            final int replicasNum = Integer.parseInt(args[4]);
            final BenchmarkInitializer initializer = new BenchmarkInitializer(url, documentCount, shardsNum, replicasNum);
            try {
                initializer.initialize();
                for (; ; ) {
                    if (initializer.aliveWriterCount.get() == 0) {
                        break;
                    }
                    Thread.sleep(1000L);
                }
            } finally {
                initializer.close();
            }
            LOG.info("Successfully initialize benchmark.");
        } else {
            final int SQLType = Integer.parseInt(args[2]);
            BenchmarkBase benchmark = null;
            try {
                switch (benchmarkType) {
                    case 1:
                        // 系统响应时间
                        final int SQLCount = Integer.parseInt(args[3]);
                        benchmark = ResponseTimeBenchmark.newInstance(url, SQLCount, SQLType);
                        break;
                    case 2:
                        // 吞吐量
                        final int concurrency = Integer.parseInt(args[3]);
                        final int timeInMinute = Integer.parseInt(args[4]);
                        final boolean isElasticsearchPrimitive = Boolean.parseBoolean(args[5]);
                        benchmark
                                = ThroughputBenchmark.newInstance(url, concurrency, timeInMinute, SQLType, isElasticsearchPrimitive);
                        break;
                    default:
                        throw new IllegalArgumentException("Argument[args[1]] is invalid, expect 1 or 2.");
                }
                benchmark.executeBenchmark();
            } finally {
                if (benchmark != null) {
                    benchmark.close();
                }
            }
        }
    }

    private static Connection getConnection(final String URL) throws SQLException {
        final Properties properties = new Properties();
        properties.put(PROPERTY_ENTRY$BENCHMARK_ENABLED.identifier, true);
        properties.put(PROPERTY_ENTRY$OUTPUT_SQL_ES_MAPPING.identifier, false);
        return DriverManager.getConnection(URL, properties);
    }

    private abstract static class BenchmarkBase {

        protected final String[] conditionFieldNames = {
                "stuBirthday", "stuChineseScore", "stuEnglishScore"
        };

        protected final int conditionFieldCount = conditionFieldNames.length;

        protected final Random random;

        protected final DataBuilder dataBuilder;

        protected BenchmarkBase() {
            this.random = new Random(System.currentTimeMillis());
            this.dataBuilder = new DataBuilder(this.random);
        }

        public abstract void executeBenchmark() throws SQLException;

        public abstract void close() throws Exception;

        protected int randomConditionFieldIndex() {
            int randomConditionFieldIndex;
            do {
                randomConditionFieldIndex = this.random.nextInt(this.conditionFieldCount);
            } while (!(randomConditionFieldIndex >= 0
                    && randomConditionFieldIndex < this.conditionFieldCount));
            return randomConditionFieldIndex;
        }

        protected String randomConditionFieldValue(final int conditionFieldIndex, final boolean isForSQL) {
            String conditionFieldValue;
            switch (conditionFieldIndex) {
                case 0:
                    final String value = this.dataBuilder.buildRandomBirthday();
                    conditionFieldValue = isForSQL ? "'" + value + "'" : value;
                    break;
                default:
                    conditionFieldValue = String.valueOf(this.dataBuilder.buildRandomScore());
            }
            return conditionFieldValue;
        }

        protected String randomConditionFieldValue(final int conditionFieldIndex) {
            return this.randomConditionFieldValue(conditionFieldIndex, true);
        }

    }

    private abstract static class ResponseTimeBenchmark extends BenchmarkBase {

        protected final Connection connection;

        protected final int SQLCount;

        protected ResponseTimeBenchmark(final String URL,
                                        final int SQLCount) throws SQLException {
            super();
            this.connection = getConnection(URL);
            this.SQLCount = SQLCount;
        }

        public static ResponseTimeBenchmark newInstance(final String URL,
                                                        final int SQLCount,
                                                        final int SQLType) throws SQLException {
            switch (SQLType) {
                case 1:
                    return new ResponseTimeWithNonAggregateQuery(URL, SQLCount);
                case 2:
                    return new ResponseTimeWithAggregateQuery(URL, SQLCount);
                default:
                    throw new IllegalArgumentException("SQLType is invalid, expect 1 or 2.");
            }
        }

        protected abstract String getExecuteSQL();

        @Override
        public final void executeBenchmark() throws SQLException {
            final Statement statement = this.connection.createStatement();
            double startTimeInMillis, totalCostTimeInMillis = 0D;
            try {
                for (int index = 0; index < this.SQLCount; index++) {
                    startTimeInMillis = System.currentTimeMillis();
                    statement.executeQuery(this.getExecuteSQL());
                    totalCostTimeInMillis += (System.currentTimeMillis() - startTimeInMillis);
                }
            } finally {
                statement.close();
            }
            final double crabsAvgCostTime = totalCostTimeInMillis / this.SQLCount;
            LOG.info("Crabs cost time(ms): " + crabsAvgCostTime);
            final org.codefamily.crabs.jdbc.Connection physicalConnection
                    = (org.codefamily.crabs.jdbc.Connection) this.connection;
            final double elasticsearchTotalCostTimeInMillis
                    = ((Number) (physicalConnection.getExecuteEnvironment().getTotalCostTimeInMillis())).doubleValue();
            final double elasticsearchAvgCostTime = elasticsearchTotalCostTimeInMillis / this.SQLCount;
            LOG.info("ES cost time(ms): " + elasticsearchAvgCostTime);
        }

        @Override
        public final void close() throws Exception {
            this.connection.close();
        }

        private static final class ResponseTimeWithNonAggregateQuery extends ResponseTimeBenchmark {

            private final String SQLTemplate = "SELECT * FROM benchmark " +
                    "WHERE ${FIELD_1} > ${FIELD_VALUE_1} AND ${FIELD_2} <= ${FIELD_VALUE_2}";

            ResponseTimeWithNonAggregateQuery(final String URL,
                                              final int SQLCount) throws SQLException {
                super(URL, SQLCount);
            }

            @Override
            protected final String getExecuteSQL() {
                final int firstConditionFieldIndex = this.randomConditionFieldIndex();
                final int secondConditionFieldIndex = this.randomConditionFieldIndex();
                return this.SQLTemplate
                        .replace("${FIELD_1}", this.conditionFieldNames[firstConditionFieldIndex])
                        .replace("${FIELD_VALUE_1}", this.randomConditionFieldValue(firstConditionFieldIndex))
                        .replace("${FIELD_2}", this.conditionFieldNames[secondConditionFieldIndex])
                        .replace("${FIELD_VALUE_2}", this.randomConditionFieldValue(secondConditionFieldIndex));
            }
        }

        private static final class ResponseTimeWithAggregateQuery extends ResponseTimeBenchmark {

            private final String SQLTemplate = "SELECT COUNT(*), AVG(stuChineseScore), MAX(stuEnglishScore) " +
                    "FROM benchmark WHERE ${FIELD_1} > ${FIELD_VALUE_1} AND ${FIELD_2} <= ${FIELD_VALUE_2} " +
                    "GROUP BY stuBirthday";

            ResponseTimeWithAggregateQuery(final String URL,
                                           final int benchmarkCount) throws SQLException {
                super(URL, benchmarkCount);
            }

            @Override
            protected final String getExecuteSQL() {
                final int firstConditionFieldIndex = this.randomConditionFieldIndex();
                final int secondConditionFieldIndex = this.randomConditionFieldIndex();
                return this.SQLTemplate
                        .replace("${FIELD_1}", this.conditionFieldNames[firstConditionFieldIndex])
                        .replace("${FIELD_VALUE_1}", this.randomConditionFieldValue(firstConditionFieldIndex))
                        .replace("${FIELD_2}", this.conditionFieldNames[secondConditionFieldIndex])
                        .replace("${FIELD_VALUE_2}", this.randomConditionFieldValue(secondConditionFieldIndex));
            }

        }

    }

    private abstract static class ThroughputBenchmark extends BenchmarkBase {

        protected final long timeInMillis;

        protected final int concurrency;

        protected final int SQLType;

        protected long completeQueryCount;

        protected double totalCostTime;

        protected ThroughputBenchmark(final int timeInMinute,
                                      final int concurrency,
                                      final int SQLType) {
            super();
            this.timeInMillis = timeInMinute * 60 * 1000L;
            this.concurrency = concurrency;
            this.SQLType = SQLType;
            this.completeQueryCount = 0L;
            this.totalCostTime = 0D;
        }

        public static ThroughputBenchmark newInstance(final String URL,
                                               final int concurrency,
                                               final int timeInMinute,
                                               final int SQLType,
                                               final boolean isElasticsearchPrimitive) throws Exception {
            if (!isElasticsearchPrimitive) {
                return new CrabsThroughputBenchmark(URL, concurrency, timeInMinute, SQLType);
            }
            final Protocol protocol = Protocol.parseURL(URL);
            final Properties properties = protocol.getProperties();
            properties.put(PROPERTY_ENTRY$BENCHMARK_ENABLED.identifier, true);
            return new ElasticsearchThroughputBenchmark(
                    protocol.getServerAddresses(),
                    protocol.getDatabaseName(),
                    "benchmark",
                    properties,
                    timeInMinute,
                    concurrency,
                    SQLType
            );
        }

        protected abstract Runnable getExecutableTask();

        protected volatile int aliveExecutorCount;

        @Override
        public final void executeBenchmark() throws SQLException {
            final Thread[] executors = new Thread[this.concurrency];
            for (int index = 0; index < this.concurrency; index++) {
                executors[index] = new Thread(this.getExecutableTask());
            }
            this.aliveExecutorCount = this.concurrency;
            for (int index = 0; index < this.concurrency; index++) {
                executors[index].start();
            }
            for (; ; ) {
                if (this.aliveExecutorCount == 0) {
                    break;
                }
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    // nothing to do.
                }
            }
            LOG.info(this.getClass().getSimpleName() + " throughput: " +
                    this.completeQueryCount / (this.timeInMillis / 1000.0D));
            LOG.info(this.getClass().getSimpleName() + " crabs response time: " +
                    this.totalCostTime / this.completeQueryCount);
        }

        private static final class CrabsThroughputBenchmark extends ThroughputBenchmark {

            private final Connection connection;

            protected CrabsThroughputBenchmark(final String URL,
                                               final int concurrency,
                                               final int timeInMinute,
                                               final int SQLType) throws SQLException {
                super(timeInMinute, concurrency, SQLType);
                this.connection = getConnection(URL);
            }

            @Override
            protected final Runnable getExecutableTask() {
                switch (this.SQLType) {
                    case 1:
                        return new NonAggregateQueryTask();
                    case 2:
                        return new AggregateQueryTask();
                    default:
                        throw new IllegalArgumentException("SQLType is invalid, expect 1 or 2.");
                }
            }

            @Override
            public final void close() throws Exception {
                this.connection.close();
            }

            private abstract class ExecutableTask implements Runnable {

                protected ExecutableTask() {
                    // nothing to do.
                }

                protected abstract String getExecuteSQL();

                @Override
                public final void run() {
                    int completeQueryCount = 0;
                    long crabsTotalCostTime = 0L;
                    try {
                        final Statement statement = CrabsThroughputBenchmark.this.connection.createStatement();
                        final long startTimeInMillis = System.currentTimeMillis();
                        long executeStartTimeInMillis;
                        try {
                            for (; ; ) {
                                executeStartTimeInMillis = System.currentTimeMillis();
                                statement.executeQuery(this.getExecuteSQL());
                                crabsTotalCostTime += (System.currentTimeMillis() - executeStartTimeInMillis);
                                completeQueryCount++;
                                if (System.currentTimeMillis() - startTimeInMillis >= CrabsThroughputBenchmark.this.timeInMillis) {
                                    break;
                                }
                            }
                        } finally {
                            statement.close();
                        }
                    } catch (SQLException e) {
                        LOG.error(e.getMessage(), e);
                    }
                    synchronized (CrabsThroughputBenchmark.this) {
                        CrabsThroughputBenchmark.this.completeQueryCount += completeQueryCount;
                        CrabsThroughputBenchmark.this.totalCostTime += crabsTotalCostTime;
                        CrabsThroughputBenchmark.this.aliveExecutorCount--;
                    }
                }

            }

            private final class NonAggregateQueryTask extends ExecutableTask {

                protected NonAggregateQueryTask() {
                    // nothing to do.
                }

                private final String SQLTemplate = "SELECT * FROM benchmark " +
                        "WHERE ${FIELD_1} > ${FIELD_VALUE_1} AND ${FIELD_2} <= ${FIELD_VALUE_2}";

                @Override
                protected final String getExecuteSQL() {
                    final int firstConditionFieldIndex = CrabsThroughputBenchmark.this.randomConditionFieldIndex();
                    final int secondConditionFieldIndex = CrabsThroughputBenchmark.this.randomConditionFieldIndex();
                    return this.SQLTemplate
                            .replace("${FIELD_1}", CrabsThroughputBenchmark.this.conditionFieldNames[firstConditionFieldIndex])
                            .replace("${FIELD_VALUE_1}", CrabsThroughputBenchmark.this.randomConditionFieldValue(firstConditionFieldIndex))
                            .replace("${FIELD_2}", CrabsThroughputBenchmark.this.conditionFieldNames[secondConditionFieldIndex])
                            .replace("${FIELD_VALUE_2}", CrabsThroughputBenchmark.this.randomConditionFieldValue(secondConditionFieldIndex));
                }

            }

            private final class AggregateQueryTask extends ExecutableTask {

                private final String SQLTemplate = "SELECT COUNT(*), AVG(stuChineseScore), MAX(stuEnglishScore) " +
                        "FROM benchmark WHERE ${FIELD_1} > ${FIELD_VALUE_1} AND ${FIELD_2} <= ${FIELD_VALUE_2} " +
                        "GROUP BY stuBirthday";

                protected AggregateQueryTask() {
                    // nothing to do.
                }

                @Override
                protected final String getExecuteSQL() {
                    final int firstConditionFieldIndex = CrabsThroughputBenchmark.this.randomConditionFieldIndex();
                    final int secondConditionFieldIndex = CrabsThroughputBenchmark.this.randomConditionFieldIndex();
                    return this.SQLTemplate
                            .replace("${FIELD_1}", CrabsThroughputBenchmark.this.conditionFieldNames[firstConditionFieldIndex])
                            .replace("${FIELD_VALUE_1}", CrabsThroughputBenchmark.this.randomConditionFieldValue(firstConditionFieldIndex))
                            .replace("${FIELD_2}", CrabsThroughputBenchmark.this.conditionFieldNames[secondConditionFieldIndex])
                            .replace("${FIELD_VALUE_2}", CrabsThroughputBenchmark.this.randomConditionFieldValue(secondConditionFieldIndex));
                }

            }

        }

        private static final class ElasticsearchThroughputBenchmark extends ThroughputBenchmark {

            private final Client elasticsearchClient;

            private final String indexIdentifier;

            private final String typeIdentifier;

            protected ElasticsearchThroughputBenchmark(final ElasticsearchAddress[] elasticsearchAddresses,
                                                       final String indexIdentifier,
                                                       final String typeIdentifier,
                                                       final Properties properties,
                                                       final int timeInMinute,
                                                       final int concurrency,
                                                       final int SQLType) {
                super(timeInMinute, concurrency, SQLType);
                ImmutableSettings.Builder builder = settingsBuilder();
                if (properties != null && !properties.isEmpty()) {
                    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                        builder.put(entry.getKey().toString(), entry.getValue());
                    }
                }
                final TransportClient client = new TransportClient(builder.build());
                for (ElasticsearchAddress address : elasticsearchAddresses) {
                    client.addTransportAddress(new InetSocketTransportAddress(address.getHost(), address.getPort()));
                }
                this.elasticsearchClient = client;
                this.indexIdentifier = indexIdentifier;
                this.typeIdentifier = typeIdentifier;
            }

            @Override
            protected Runnable getExecutableTask() {
                switch (this.SQLType) {
                    case 1:
                        return new NonAggregateQueryTask();
                    case 2:
                        return new AggregateQueryTask();
                    default:
                        throw new IllegalArgumentException("SQLType is invalid, expect 1 or 2.");
                }
            }

            @Override
            public final void close() throws Exception {
                this.elasticsearchClient.close();
            }

            private abstract class ExecutableTask<Request extends ActionRequest, Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>,
                    TAction extends Action<Request, Response, RequestBuilder>> implements Runnable {

                protected ExecutableTask() {
                    // nothing to do.
                }

                protected abstract TAction buildAction();

                protected abstract Request buildRequest();

                @Override
                public final void run() {
                    int completeQueryCount = 0;
                    long totalCostTime = 0L;
                    long executeStartTimeInMillis;
                    final long startTimeInMillis = System.currentTimeMillis();
                    for (; ; ) {
                        executeStartTimeInMillis = System.currentTimeMillis();
                        ElasticsearchThroughputBenchmark.this.elasticsearchClient.execute(
                                this.buildAction(),
                                this.buildRequest()
                        ).actionGet();
                        totalCostTime += (System.currentTimeMillis() - executeStartTimeInMillis);
                        completeQueryCount++;
                        if (System.currentTimeMillis() - startTimeInMillis >= ElasticsearchThroughputBenchmark.this.timeInMillis) {
                            break;
                        }
                    }
                    synchronized (ElasticsearchThroughputBenchmark.this) {
                        ElasticsearchThroughputBenchmark.this.completeQueryCount += completeQueryCount;
                        ElasticsearchThroughputBenchmark.this.totalCostTime += totalCostTime;
                        ElasticsearchThroughputBenchmark.this.aliveExecutorCount--;
                    }
                }

            }

            private final class NonAggregateQueryTask
                    extends ExecutableTask<SearchRequest, SearchResponse, SearchRequestBuilder, SearchAction> {

                private final String[] INCLUDES = new String[]{
                        "stuBirthday", "stuChineseScore", "stuClass", "stuEnglishScore", "stuName", "stuNo"
                };

                private final String[] EXCLUDES = new String[0];

                protected NonAggregateQueryTask() {
                    super();
                }

                @Override
                protected final SearchAction buildAction() {
                    return SearchAction.INSTANCE;
                }

                @Override
                protected final SearchRequest buildRequest() {
                    final int firstConditionFieldIndex = randomConditionFieldIndex();
                    final int secondConditionFieldIndex = randomConditionFieldIndex();
                    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                    searchSourceBuilder.fetchSource(
                            INCLUDES, EXCLUDES
                    );
                    searchSourceBuilder.from(0).size(500);
                    searchSourceBuilder.query(
                            new FilteredQueryBuilder(
                                    QueryBuilders.matchAllQuery(),
                                    FilterBuilders.andFilter(
                                            FilterBuilders
                                                    .rangeFilter(conditionFieldNames[firstConditionFieldIndex])
                                                    .gt(randomConditionFieldValue(firstConditionFieldIndex, false)),
                                            FilterBuilders
                                                    .rangeFilter(conditionFieldNames[secondConditionFieldIndex])
                                                    .lte(randomConditionFieldValue(secondConditionFieldIndex, false))
                                    )
                            )

                    );
                    final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(elasticsearchClient);
                    searchRequestBuilder.internalBuilder(searchSourceBuilder);
                    searchRequestBuilder.setIndices(indexIdentifier);
                    searchRequestBuilder.setTypes(typeIdentifier);
                    return searchRequestBuilder.request();
                }
            }

            private final class AggregateQueryTask
                    extends ExecutableTask<SearchRequest, SearchResponse, SearchRequestBuilder, SearchAction> {

                protected AggregateQueryTask() {
                    super();
                }

                @Override
                protected final SearchAction buildAction() {
                    return SearchAction.INSTANCE;
                }

                @Override
                protected final SearchRequest buildRequest() {
                    final int firstConditionFieldIndex = randomConditionFieldIndex();
                    final int secondConditionFieldIndex = randomConditionFieldIndex();
                    final SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                    searchSourceBuilder.query(
                            new FilteredQueryBuilder(
                                    QueryBuilders.matchAllQuery(),
                                    FilterBuilders.andFilter(
                                            FilterBuilders
                                                    .rangeFilter(conditionFieldNames[firstConditionFieldIndex])
                                                    .gt(randomConditionFieldValue(firstConditionFieldIndex, false)),
                                            FilterBuilders
                                                    .rangeFilter(conditionFieldNames[secondConditionFieldIndex])
                                                    .lte(randomConditionFieldValue(secondConditionFieldIndex, false))
                                    )
                            )

                    );
                    searchSourceBuilder.aggregation(
                            AggregationBuilders
                                    .terms("stuBirthday")
                                    .field("stuBirthday")
                                    .size(0)
                                    .subAggregation(AggregationBuilders.count("0").field("stuNo"))
                                    .subAggregation(AggregationBuilders.avg("1").field("stuChineseScore"))
                                    .subAggregation(AggregationBuilders.max("2").field("stuEnglishScore"))
                    );
                    final SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(elasticsearchClient);
                    searchRequestBuilder.internalBuilder(searchSourceBuilder);
                    searchRequestBuilder.setIndices(indexIdentifier);
                    searchRequestBuilder.setTypes(typeIdentifier);
                    return searchRequestBuilder.request();
                }
            }

        }

    }

    static final class DataBuilder {

        private final char[] characters = {
                'A', 'B', 'C', 'D', 'E', 'F', 'G',
                'H', 'I', 'J', 'K', 'L', 'M', 'N',
                'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                'V', 'W', 'X', 'Y', 'Z',
                '1', '2', '3', '4', '5', '6', '7', '8', '9'
        };

        private final int characterLength = this.characters.length;

        private final char[] digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};

        private final int digitLength = this.digits.length;

        private final char[] letters = {
                'A', 'B', 'C', 'D', 'E', 'F', 'G',
                'H', 'I', 'J', 'K', 'L', 'M', 'N',
                'O', 'P', 'Q', 'R', 'S', 'T', 'U',
                'V', 'W', 'X', 'Y', 'Z'
        };

        private final int letterLength = this.letters.length;

        private final Random random;

        private final StringBuilder builder;

        private DataBuilder(final Random random) {
            this.random = random;
            this.builder = new StringBuilder(10);
        }

        public final String buildRandomBirthday() {
            final int startYear = 1984, endYear = 1984 + 21;
            int year;
            do {
                year = this.random.nextInt(endYear);
            } while (!(year >= startYear && year < endYear));
            int month;
            do {
                month = this.random.nextInt(13);
            } while (!(month >= 1 && month <= 12));
            int day;
            do {
                day = this.random.nextInt(29);
            } while (!(day >= 1 && day <= 28));
            return year + "-" + month + "-" + day;
        }

        public final double buildRandomScore() {
            double chineseScore;
            do {
                chineseScore = this.random.nextDouble() * 100;
            } while (!(chineseScore >= 45.0D && chineseScore <= 100.0D));
            final DecimalFormat decimalFormat = new DecimalFormat("#.00");
            return Double.parseDouble(decimalFormat.format(chineseScore));
        }

        public final BenchmarkInitializer.Document nextDocument() {
            return new BenchmarkInitializer.Document(
                    this.buildRandomStuNo(),
                    this.buildRandomName(),
                    this.buildRandomStuClass(),
                    this.buildRandomBirthday(),
                    this.buildRandomScore(),
                    this.buildRandomScore()
            );
        }

        private String buildRandomStuClass() {
            this.builder.setLength(0);
            int letterIndex;
            do {
                letterIndex = this.random.nextInt(this.letterLength);
            } while (!(letterIndex >= 0 && letterIndex < this.letterLength));
            this.builder.append(this.letters[letterIndex]);
            this.builder.append("-");
            for (int index = 0; index < 5; index++) {
                int digitIndex;
                do {
                    digitIndex = this.random.nextInt(this.digitLength);
                } while (!(digitIndex >= 0 && digitIndex < this.digitLength));
                this.builder.append(this.digits[digitIndex]);
            }
            return this.builder.toString();
        }

        private String buildRandomName() {
            this.builder.setLength(0);
            int characterIndex;
            for (int index = 0; index < 10; index++) {
                do {
                    characterIndex = this.random.nextInt(this.characterLength);
                } while (!(characterIndex >= 0 && characterIndex < this.characterLength));
                this.builder.append(this.characters[characterIndex]);
            }
            return this.builder.toString();
        }

        private long lastStuNo = 100000000L;

        private String buildRandomStuNo() {
            lastStuNo += 1;
            return String.valueOf(lastStuNo);
        }

    }

    static final class BenchmarkInitializer implements Closeable{

        private static final int DATA_WRITER_CONCURRENCY = 10;

        private final Object lock = new Object();

        private final AtomicLong counter = new AtomicLong(0);

        private final int dataBufferSize = 10000;

        private final AdvancedClient advancedClient;

        private final int documentCount;

        private final int shardsNum;

        private final int replicasNum;

        private final LinkedBlockingQueue<Document> dataBuffer;

        private BenchmarkInitializer(final String URL,
                                     final int documentCount,
                                     final int shardsNum,
                                     final int replicasNum) throws SQL4ESException {
            final Protocol protocol = Protocol.parseURL(URL);
            final ElasticsearchAddress[] elasticsearchAddresses = protocol.getServerAddresses();
            final Properties properties = protocol.getProperties();
            this.advancedClient = new AdvancedClient(elasticsearchAddresses, properties);
            this.documentCount = documentCount;
            this.shardsNum = shardsNum;
            this.replicasNum = replicasNum;
            this.dataBuffer = new LinkedBlockingQueue<Document>(dataBufferSize);
        }

        private boolean hasMoreData = true;

        private AtomicInteger aliveWriterCount;

        public final void initialize() throws Exception {
            final IndexDefinition indexDefinition = new IndexDefinition(
                    new Identifier("benchmark"),
                    this.shardsNum,
                    this.replicasNum
            );
            final TypeDefinition typeDefinition = new TypeDefinition(
                    indexDefinition,
                    new Identifier("benchmark"),
                    false,
                    false
            );
            typeDefinition.defineStringField(new Identifier("stuNo")).asPrimaryField();
            typeDefinition.defineStringField(new Identifier("stuName"));
            typeDefinition.defineStringField(new Identifier("stuClass"));
            typeDefinition.defineDateField(new Identifier("stuBirthday"), "yyyy-MM-dd");
            typeDefinition.defineDoubleField(new Identifier("stuChineseScore"));
            typeDefinition.defineDoubleField(new Identifier("stuEnglishScore"));
            typeDefinition.publish();
            if (this.advancedClient.existsIndex(indexDefinition.getIdentifier())) {
                this.advancedClient.dropIndex(indexDefinition.getIdentifier());
            }
            this.advancedClient.createIndex(indexDefinition);
            if (this.advancedClient.existsType(typeDefinition)) {
                this.advancedClient.dropType(typeDefinition);
            }
            this.advancedClient.createType(typeDefinition);
            LOG.info("Successfully create index[" + indexDefinition.getIdentifier() +
                    "] and type[" + typeDefinition.getIdentifier() + "].");
            final DataBuilderTask dataBuilderTask = new DataBuilderTask(this.documentCount);
            final DataWriterTask[] dataWriterTasks = new DataWriterTask[DATA_WRITER_CONCURRENCY];
            for (int index = 0; index < DATA_WRITER_CONCURRENCY; index++) {
                dataWriterTasks[index] = new DataWriterTask();
            }
            dataBuilderTask.start();
            this.aliveWriterCount = new AtomicInteger(DATA_WRITER_CONCURRENCY);
            for (int index = 0; index < DATA_WRITER_CONCURRENCY; index++) {
                dataWriterTasks[index].start();
            }
            dataBuilderTask.join();

        }

        @Override
        public final void close() throws IOException {
            this.advancedClient.close();
        }

        final class DataBuilderTask extends Thread {

            private final int documentCount;

            private final DataBuilder dataBuilder;

            DataBuilderTask(final int documentCount) {
                this.documentCount = documentCount;
                this.dataBuilder = new DataBuilder(new Random(System.currentTimeMillis()));
            }

            @Override
            public void run() {
                Document data;
                for (int index = 0; index < this.documentCount; index++) {
                    data = this.dataBuilder.nextDocument();
                    final long currentCount = BenchmarkInitializer.this.counter.incrementAndGet();
                    if (currentCount > BenchmarkInitializer.this.dataBufferSize) {
                        synchronized (BenchmarkInitializer.this.lock) {
                            if (BenchmarkInitializer.this.counter.get() > BenchmarkInitializer.this.dataBufferSize) {
                                LOG.warn("Queue in " + BenchmarkInitializer.class.getSimpleName() +
                                        " is full, so the thread which build document will be blocked.");
                                try {
                                    BenchmarkInitializer.this.lock.wait();
                                } catch (InterruptedException e) {
                                    // nothing to do.
                                }
                            }
                        }
                    }
                    BenchmarkInitializer.this.dataBuffer.offer(data);
                }
                BenchmarkInitializer.this.hasMoreData = false;
            }
        }

        final class DataWriterTask extends Thread {

            private static final long WAIT_FOR_TIMEOUT = 1000L;

            private static final int BULK_SIZE = 1000;

            private final ArrayList<Document> bulkDocumentList;

            DataWriterTask() {
                this.bulkDocumentList = new ArrayList<Document>(BULK_SIZE);
            }

            @Override
            public final void run() {
                int count = 0;
                for (; ; ) {
                    try {
                        final Document data
                                = BenchmarkInitializer.this.dataBuffer.poll(WAIT_FOR_TIMEOUT, TimeUnit.MILLISECONDS);
                        if (data == null && !BenchmarkInitializer.this.hasMoreData) {
                            if (this.bulkDocumentList.size() > 0) {
                                // 在退出之前将未提交的数据提交到ES
                                BenchmarkInitializer.this.advancedClient.execute(
                                        new InternalDocumentBuilder$BulkRequest(),
                                        new InternalDocumentResponseCallback(),
                                        this.bulkDocumentList
                                );
                                LOG.info(Thread.currentThread().getName() +
                                        " already put data -> " + this.bulkDocumentList.size());
                            }
                            break;
                        }
                        final long currentCount = BenchmarkInitializer.this.counter.decrementAndGet();
                        if (currentCount == BenchmarkInitializer.this.dataBufferSize - 1) {
                            synchronized (BenchmarkInitializer.this.lock) {
                                LOG.warn("Queue in " + BenchmarkInitializer.this.getClass().getSimpleName() +
                                        " is not full, so notify the thread which writes data to queue.");
                                BenchmarkInitializer.this.lock.notify();
                            }
                        }
                        count++;
                        this.bulkDocumentList.add(data);
                        if (count == BULK_SIZE || !BenchmarkInitializer.this.hasMoreData) {
                            BenchmarkInitializer.this.advancedClient.execute(
                                    new InternalDocumentBuilder$BulkRequest(),
                                    new InternalDocumentResponseCallback(),
                                    this.bulkDocumentList
                            );
                            LOG.info(Thread.currentThread().getName() +
                                    " already put data -> " + this.bulkDocumentList.size());
                            this.bulkDocumentList.clear();
                            count = 0;
                        }
                    } catch (InterruptedException e) {
                        // nothing to do.
                    } catch (SQL4ESException e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
                LOG.info(Thread.currentThread().getName() + " is died.");
                BenchmarkInitializer.this.aliveWriterCount.decrementAndGet();
            }

            private final class InternalDocumentBuilder$BulkRequest implements
                    InternalDocumentRequestBuilder<BulkRequest, BulkResponse,
                            BulkRequestBuilder, BulkAction, ArrayList<Document>> {
                @Override
                public final BulkAction buildAction() {
                    return BulkAction.INSTANCE;
                }

                @Override
                public final BulkRequest buildRequest(final Client client,
                                                      final ArrayList<Document> value) throws SQL4ESException {
                    final BulkRequestBuilder requestBuilder = client.prepareBulk();
                    for (Document data : value) {
                        try {
                            requestBuilder.add(
                                    client.prepareIndex("benchmark", "benchmark")
                                            .setSource(
                                                    jsonBuilder()
                                                            .startObject()
                                                            .field("stuNo", data.stuNo)
                                                            .field("stuName", data.stuName)
                                                            .field("stuClass", data.stuClass)
                                                            .field("stuBirthday", data.stuBirthday)
                                                            .field("stuChineseScore", data.stuChineseScore)
                                                            .field("stuEnglishScore", data.stuEnglishScore)
                                                            .endObject()
                                            )
                            );
                        } catch (IOException e) {
                            throw new SQL4ESException(e.getMessage(), e);
                        }
                    }
                    return requestBuilder.request();
                }
            }

            private final class InternalDocumentResponseCallback implements ResponseCallback<BulkResponse> {
                @Override
                public final void callback(final BulkResponse response) throws SQL4ESException {
                    if (response.hasFailures()) {
                        throw new SQL4ESException(response.buildFailureMessage());
                    }
                }
            }
        }

        public static final class Document {

            private final String stuNo;

            private final String stuName;

            private final String stuClass;

            private final String stuBirthday;

            private final double stuChineseScore;

            private final double stuEnglishScore;

            Document(final String stuNo,
                     final String stuName,
                     final String stuClass,
                     final String stuBirthday,
                     final double stuChineseScore,
                     final double stuEnglishScore) {
                this.stuNo = stuNo;
                this.stuName = stuName;
                this.stuClass = stuClass;
                this.stuBirthday = stuBirthday;
                this.stuChineseScore = stuChineseScore;
                this.stuEnglishScore = stuEnglishScore;
            }

        }

    }


}
