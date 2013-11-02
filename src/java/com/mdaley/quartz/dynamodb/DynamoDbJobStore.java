package com.mdaley.quartz.dynamodb;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDbJobStore implements JobStore, Constants {
    private static final Logger log = LoggerFactory.getLogger(com.mdaley.quartz.dynamodb.DynamoDbJobStore.class);

    private ClassLoadHelper loadHelper;
    private SchedulerSignaler schedulerSignaler;
    private String dynamoDbUrl;
    private String quartzPrefix = "QRTZ_";

    private ClientConfiguration clientConfig = new ClientConfiguration();

    private AmazonDynamoDBClient client;

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedulerSignaler) throws SchedulerConfigException {
        this.loadHelper = loadHelper;
        this.schedulerSignaler = schedulerSignaler;

        if (dynamoDbUrl == null) {
            throw new SchedulerConfigException("DynamoDB location must be set");
        }

        log.info(String.format("DynamoDb: location: '%s', table prefix: '%s'", dynamoDbUrl, quartzPrefix));

        configureAwsCredentials();
        configureClient();

        createClient();

        ensureTables();
    }

    private void ensureTables() {
        initializeHashAndRangeTable("jobs", "name", "group");
        initializeHashAndRangeTable("triggers", "name", "group");
        initializeHashTable("calendars", "name");
        initializeHashAndRangeTable("locks", "name", "group");
        initializeHashTable("paused_job_groups", "group");
        initializeHashTable("paused_trigger_groups", "group");
    }

    private void initializeHashAndRangeTable(String name, String hashName, String rangeName) {
        String tableName = quartzPrefix + name;

        if (!tableExists(tableName)) {
            log.info(String.format("Creating table '%s' with hash and range index.", tableName));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(
                            new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(hashName),
                            new KeySchemaElement().withKeyType(KeyType.RANGE).withAttributeName(rangeName))
                    .withAttributeDefinitions(
                            new AttributeDefinition(hashName, ScalarAttributeType.S),
                            new AttributeDefinition(rangeName, ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

            client.createTable(request);

            waitForTable(tableName);
        } else {
            log.info(String.format("Table '%s' already exists.", tableName));
        }
    }

    private void initializeHashTable(String name, String hashName) {
        String tableName = quartzPrefix + name;

        if (!tableExists(tableName)) {
            log.info(String.format("Creating table '%s' with hash index.", tableName));
            CreateTableRequest request = new CreateTableRequest()
                    .withTableName(tableName)
                    .withKeySchema(
                            new KeySchemaElement().withKeyType(KeyType.HASH).withAttributeName(hashName))
                    .withAttributeDefinitions(
                            new AttributeDefinition(hashName, ScalarAttributeType.S))
                    .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

            client.createTable(request);

            waitForTable(tableName);
        } else {
            log.info(String.format("Table '%s' already exists.", tableName));
        }
    }

    private void sleep(long millis) {
        try {Thread.sleep(millis);
        } catch (Exception e) {
            // nop
        }
    }

    private void waitForTable(String name) {
        log.info(String.format("Waiting for creation of table '%s' to complete.", name));
        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            sleep(1000 * 20);
            try {
                DescribeTableRequest request = new DescribeTableRequest().withTableName(name);
                TableDescription tableDescription = client.describeTable(request).getTable();
                String tableStatus = tableDescription.getTableStatus();
                log.info(String.format("Table '%s' is in state: '%s'.", name, tableStatus));
                if (tableStatus.equals(TableStatus.ACTIVE.toString())) {
                    return;
                }
            } catch (ResourceNotFoundException e) {
                // nop - maybe the table isn't showing up yet.
            }
        }

        throw new RuntimeException(String.format("Table '%s' never went active.", name));
    }

    private boolean tableExists(String name) {
        try {
            client.describeTable(new DescribeTableRequest().withTableName(name));
        } catch (ResourceNotFoundException e) {
            return false;
        }

        return true;
    }

    private void createClient() {
        client = new AmazonDynamoDBClient(clientConfig);
        client.setEndpoint(dynamoDbUrl);
    }

    private void configureAwsCredentials() {
        Properties props = System.getProperties();
        String awsAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretKey = System.getenv("AWS_SECRET_KEY");
        if (awsAccessKey != null) {
            props.setProperty("aws.accessKeyId", awsAccessKey);
            log.info(String.format("Setting aws.accessKeyId to '%s'", awsAccessKey));
        }
        if (awsSecretKey != null) {
            props.setProperty("aws.secretKey", awsSecretKey);
            log.info(String.format("Setting aws.secretKey to '%s'", awsSecretKey));
        }
    }

    private void configureClient() {
        String awsProxyHost = System.getenv("AWS_HTTP_PROXY_HOST");
        String awsProxyPort = System.getenv("AWS_HTTP_PROXY_PORT");
        if (awsProxyHost != null && awsProxyPort != null) {
            clientConfig.setProxyHost(awsProxyHost);
            clientConfig.setProxyPort(Integer.parseInt(awsProxyPort));
            log.info(String.format("AWS Proxy set to host: '%s', port: '%s'", awsProxyHost, awsProxyPort));
        }
    }

    public void setDynamoDbDetails(String dynamoDbUrl, String dynamoDbTablePrefix) {
        this.dynamoDbUrl = dynamoDbUrl;
        if (dynamoDbTablePrefix != null) {
            this.quartzPrefix = dynamoDbTablePrefix;
        }
    }

    private void storeJobInDynamoDb(JobDetail job, boolean replaceExisting) {

        JobKey key = job.getKey();
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(key.getName()));
        item.put("group", new AttributeValue(key.getGroup()));
        item.put("description", new AttributeValue(job.getDescription()));

        PutItemRequest request = new PutItemRequest()
                .withTableName(quartzPrefix + "jobs")
                .withItem(item);

        client.putItem(request);
    }

    @Override
    public void schedulerStarted() throws SchedulerException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void schedulerPaused() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void schedulerResumed() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void shutdown() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsPersistence() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isClustered() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void storeJobAndTrigger(JobDetail jobDetail, OperableTrigger operableTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void storeJob(JobDetail jobDetail, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        storeJobInDynamoDb(jobDetail, replaceExisting);
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> jobDetailSetMap, boolean b) throws ObjectAlreadyExistsException, JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void storeTrigger(OperableTrigger operableTrigger, boolean b) throws ObjectAlreadyExistsException, JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger operableTrigger) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void storeCalendar(String auditHeader, Calendar calendar, boolean b, boolean b2) throws ObjectAlreadyExistsException, JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeCalendar(String auditHeader) throws JobPersistenceException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Calendar retrieveCalendar(String auditHeader) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> triggerKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> jobKeyGroupMatcher) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void pauseAll() throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void resumeAll() throws JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<OperableTrigger> acquireNextTriggers(long l, int i, long l2) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void releaseAcquiredTrigger(OperableTrigger operableTrigger) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> operableTriggers) throws JobPersistenceException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void triggeredJobComplete(OperableTrigger operableTrigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction completedExecutionInstruction) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setInstanceId(String auditHeader) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setInstanceName(String auditHeader) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setThreadPoolSize(int i) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
