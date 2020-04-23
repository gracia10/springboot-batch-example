package com.community.batch.jobs.inactive;

import java.time.LocalDateTime;
import java.util.List;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.listener.InactiveChunkListener;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InativeStepListener;
import com.community.batch.repository.UserRepository;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class InactiveUserJobConfig {

	private final static int CHUNK_SIZE = 6;
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final EntityManagerFactory entityManagerFactory;
	private final UserRepository userRepository;
	
	@Bean
	public TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor("Batch_Task");
	}
	
	@Bean
	public Job inactiveUserJob(InactiveIJobListener inactiveIJobListener, Step parititionerStep) { 
		return jobBuilderFactory.get("inactiveUserJob")
				.preventRestart()
				.listener(inactiveIJobListener)
				.start(parititionerStep)
				.build();
	}

    @Bean
    public Step parititionerStep(Step inactiveUserStep) {
        return stepBuilderFactory.get("parititionerStep")
                .partitioner("parititionerStep", new InactiveUserPartitioner())
                .gridSize(5)
                .step(inactiveUserStep)
                .taskExecutor(taskExecutor())
                .build();
    }
    
	@Bean
	public Step inactiveUserStep(ListItemReader<User> inactiveUserReader,InactiveChunkListener inactiveChunkListener, InativeStepListener inativeStepListener) {
		return stepBuilderFactory.get("inactiveUserStep")
				.<User, User>chunk(CHUNK_SIZE)
				.reader(inactiveUserReader)
				.processor(inactiveUserProcesser())
				.writer(inactiveUserWriter())
				.listener(inativeStepListener)
				.listener(inactiveChunkListener)
				.build();
	}
    
	// the stepExecutionContext only within a bean defined in the scope="step"
	@Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(@Value("#{stepExecutionContext[grade]}") String grade) {
        List<User> inactiveUsers = userRepository.findByCreatedDateBeforeAndStatusEqualsAndGradeEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE, Grade.valueOf(grade));
        return new ListItemReader<>(inactiveUsers);
    }
	
	private ItemProcessor<User, User> inactiveUserProcesser() {
		return User::setInactive;
	}
	
	private JpaItemWriter<User> inactiveUserWriter() {
		JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
		jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
		return jpaItemWriter;
	}
}
