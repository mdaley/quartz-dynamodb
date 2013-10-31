package com.mdaley.quartz.dynamodb;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import com.mdaley.quartz.dynamodb.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamoDBJobStore implements JobStore, Constants {
    private static final Logger log = LoggerFactory.getLogger(DynamoDBJobStore.class);

    private ClassLoadHelper loadHelper;
    private SchedulerSignaler schedulerSignaler;
    private String dynamoDbUrl;
    private String awsAccessKey;
    private String awsSecretKey;

    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler schedulerSignaler) throws SchedulerConfigException {
        this.loadHelper = loadHelper;
        this.schedulerSignaler = schedulerSignaler;

        if (dynamoDbUrl == null | awsAccessKey == null | awsSecretKey == null) {
            throw new SchedulerConfigException("DynamoDB location and AWS access and secret keys must be set");
        }
    }

    public void setDynamoDbDetails(String dynamoDbUrl, String awsAccessKey, String awsSecretKey) {
        this.dynamoDbUrl = dynamoDbUrl;
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
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
    public void storeJob(JobDetail jobDetail, boolean b) throws ObjectAlreadyExistsException, JobPersistenceException {
        //To change body of implemented methods use File | Settings | File Templates.
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
