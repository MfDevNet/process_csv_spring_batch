package ir.farahani.prcesscsv;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

@Configuration
@EnableBatchProcessing
@Data
@RequiredArgsConstructor
@Slf4j
public class BatchConfiguration {

    private final int chunkSize = 1000; // اندازه chunk (1000 خط)
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final JobCompletionNotificationListener listener;
    public static final Map<String, AtomicLong> duplicateRecords = new ConcurrentHashMap<>();
    public static LongAdder totalUniqueSalary = new LongAdder();
    public static LongAdder totalSalary = new LongAdder();

    @Bean
    public Job processJob() {
        return jobBuilderFactory.get("processJob")
//                .incrementer(new RunIdIncrementer())
                .listener(listener)
                .start(processStep())
                .next(finalStep())
                .build();
    }

    @Bean
    public Step processStep() {
        return stepBuilderFactory.get("processStep")
                .<Employee, Employee>chunk(chunkSize)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public FlatFileItemReader<Employee> reader() {
        return new FlatFileItemReaderBuilder<Employee>()
                .name("employeeItemReader")
                .resource(new FileSystemResource("src/main/resources/data.csv"))
                .delimited()
                .names("nationalId", "account", "salary")
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Employee.class);
                }})
                .build();
    }

    @Bean
    public ItemProcessor<Employee, Employee> processor() {
        return employee -> {
            totalSalary.add(employee.getSalary());
            String key = employee.getAccount() + "_" + employee.getSalary();
            duplicateRecords.compute(key, (k, v) -> {
                if (v == null) {
                    totalUniqueSalary.add(employee.getSalary());
                    return new AtomicLong(1);
                } else {
                    v.incrementAndGet();
                    return v;
                }
            });
            return employee;
        };
    }


    @Bean
    public Step finalStep() {
        return stepBuilderFactory.get("finalStep")
                .tasklet(finalTasklet())
                .build();
    }

    @Bean
    public Tasklet finalTasklet() {
        return (contribution, chunkContext) -> {
            log.info("Total full salary: {}", totalSalary.sum());
            log.info("Total salary of unique records: {}", totalUniqueSalary.sum());
            log.info("Duplicate records based on account and salary:");
            BatchConfiguration.duplicateRecords.forEach((key, value) -> {
                if (value.get() > 1) {
                    log.info("{}: {}", key, value);
                }
            });
            return RepeatStatus.FINISHED;
        };
    }


    @Bean
    public ItemWriter<Employee> writer() {
        return employees -> {
            // todo : change or write data another source
        };
    }

    @Bean
    public SimpleAsyncTaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("batch");
        executor.setConcurrencyLimit(100); // تعداد حداکثر thread‌های موازی برای chunk‌ها
        return executor;
    }
}
