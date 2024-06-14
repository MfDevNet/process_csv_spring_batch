package ir.farahani.prcesscsv;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class JobCompletionNotificationListener implements JobExecutionListener {

    private long startTime;
    private final JobOperator jobOperator;

    @Override
    public void beforeJob(JobExecution jobExecution) {
        if (jobExecution.getJobInstance().getJobName().equals("processJob")) {
            log.info("Job started");
        }
        startTime = System.currentTimeMillis();

    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getJobInstance().getJobName().equals("processJob")) {
            log.info("Job ended");
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            log.info("Job completed in " + duration + " milliseconds");


        }
    }
}
