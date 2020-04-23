package com.community.batch.jobs.inactive;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.listener.InactiveChunkListener;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InativeStepListener;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class InactiveUserJobConfig {

	private final static int CHUNK_SIZE = 6;
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final EntityManagerFactory entityManagerFactory;
	
	@Bean
	public TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor("Batch_Task");
	}
	
	@Bean
	public Job inactiveUserJob(InactiveIJobListener inactiveIJobListener,Step inactiveUserStep) {
		return jobBuilderFactory.get("inactiveUserJob")
				.preventRestart()
				.listener(inactiveIJobListener)
				.start(inactiveUserStep)
				.build();
	}

	@Bean
	@JobScope
	public Step inactiveUserStep(InactiveChunkListener inactiveChunkListener, InativeStepListener inativeStepListener,TaskExecutor taskExecutor) {
		return stepBuilderFactory.get("inactiveUserStep")
				.<User, User>chunk(CHUNK_SIZE)
				.reader(inactiveUserReader(null))
				.processor(inactiveUserProcesser())
				.writer(inactiveUserWriter())
				.listener(inativeStepListener)
				.listener(inactiveChunkListener)
				.taskExecutor(taskExecutor)
				.throttleLimit(2)
				.build();
	}

	@Bean
	@StepScope
	public JpaPagingItemReader<User> inactiveUserReader(@Value("#{jobParameters[nowDate]}") Date nowDate) {
		
		JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader<User>() {
			@Override
			public int getPage() {
				return 0;
			}
		};
		
		jpaPagingItemReader
		.setQueryString("select u from User as u where u.updatedDate < :updatedDate and u.status = :status");
		
		Map<String, Object> map = new HashMap<>();
		map.put("updatedDate", LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault()).minusYears(1));
		map.put("status", UserStatus.ACTIVE);
		
		jpaPagingItemReader.setParameterValues(map);
		jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
		jpaPagingItemReader.setPageSize(CHUNK_SIZE);
		jpaPagingItemReader.setSaveState(false);
		
		return jpaPagingItemReader;
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
