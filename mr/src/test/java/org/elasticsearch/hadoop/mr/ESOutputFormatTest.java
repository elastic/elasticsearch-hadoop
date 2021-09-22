package org.elasticsearch.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.elasticsearch.hadoop.cfg.Settings;
import org.elasticsearch.hadoop.rest.RestRepository;
import org.elasticsearch.hadoop.rest.RestService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

public class ESOutputFormatTest {

    @Test
    public void testXOpaqueId() throws Exception {
        //New API:
        TaskAttemptContext taskAttemptContext = Mockito.mock(TaskAttemptContext.class);
        String newApiJobTrackerId = "newApiJobTracker";
        int jobId = 34;
        int taskId = 56;
        int taskAttempt = 2;
        TaskAttemptID newApiTaskAttemptID = new TaskAttemptID(newApiJobTrackerId, jobId, true, taskId, taskAttempt);
        Mockito.when(taskAttemptContext.getTaskAttemptID()).thenReturn(newApiTaskAttemptID);
        Job job = new Job(new Configuration());
        String newApiJobName = "New API Job";
        job.setJobName(newApiJobName);
        Mockito.when(taskAttemptContext.getConfiguration()).thenReturn(job.getConfiguration());
        RestRepository restRepository = Mockito.mock(RestRepository.class);
        RestService.PartitionWriter partitionWriter = Mockito.mock(RestService.PartitionWriter.class);
        Mockito.when(partitionWriter.getRepository()).thenReturn(restRepository);
        AtomicReference<Settings> resultSettingsAtomicReference = new AtomicReference<>();
        EsOutputFormat.PartitionWriterProvider partitionWriterProvider = (settings, currentSplit, totalSplits, log) -> {
            resultSettingsAtomicReference.set(settings);
            return partitionWriter;
        };
        EsOutputFormat esOutputFormat = new EsOutputFormat(partitionWriterProvider);
        RecordWriter recordWriter = esOutputFormat.getRecordWriter(taskAttemptContext);
        recordWriter.write("a", "b");
        Settings resultSettings = resultSettingsAtomicReference.get();
        String opaqueId = resultSettings.getProperty("es.net.http.header.X-Opaque-ID");
        Assert.assertEquals(String.format(Locale.ROOT, "mapreduce %s %s", newApiJobName, newApiTaskAttemptID), opaqueId);

        //Old API:
        String oldApiJobTrackerId = "oldApiJobTracker";
        TaskAttemptID oldApiTaskAttemptID = new TaskAttemptID(oldApiJobTrackerId, jobId, true, taskId, taskAttempt);
        JobConf jobConf = new JobConf();
        jobConf.set("mapreduce.task.attempt.id", oldApiTaskAttemptID.toString());
        String oldApiJobName = "Old API Job name";
        jobConf.setJobName(oldApiJobName);
        org.apache.hadoop.mapred.RecordWriter oldApiRecordWriter = esOutputFormat.getRecordWriter(null, jobConf, "", null);
        oldApiRecordWriter.write("a", "b");
        resultSettings = resultSettingsAtomicReference.get();
        opaqueId = resultSettings.getProperty("es.net.http.header.X-Opaque-ID");
        Assert.assertEquals(String.format(Locale.ROOT, "mapreduce %s %s", oldApiJobName, oldApiTaskAttemptID), opaqueId);

        //No job name:
        jobConf = new JobConf();
        jobConf.set("mapreduce.task.attempt.id", oldApiTaskAttemptID.toString());
        oldApiRecordWriter = esOutputFormat.getRecordWriter(null, jobConf, "", null);
        oldApiRecordWriter.write("a", "b");
        resultSettings = resultSettingsAtomicReference.get();
        opaqueId = resultSettings.getProperty("es.net.http.header.X-Opaque-ID");
        String missingJobName = "job";
        Assert.assertEquals(String.format(Locale.ROOT, "mapreduce %s %s", missingJobName, oldApiTaskAttemptID), opaqueId);
    }
}
