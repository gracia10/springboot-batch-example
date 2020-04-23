package com.community.batch.jobs.inactive;

import java.time.LocalDateTime;
import java.util.List;

import javax.persistence.EntityManagerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.listener.InactiveChunkListener;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InativeStepListener;
import com.community.batch.jobs.readers.QueueItemReader;
import com.community.batch.repository.UserRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Configuration
public class InactiveUserJobConfig {

	private final static int CHUNK_SIZE = 6;
	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	
	private final UserRepository userRepository;
	
	@Primary
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
	public Step inactiveUserStep(InactiveChunkListener inactiveChunkListener, InativeStepListener inativeStepListener) {
		return stepBuilderFactory.get("inactiveUserStep")
				.<User, User>chunk(CHUNK_SIZE)
				.reader(inactiveUserReader())
				.processor(inactiveUserProcessor())
				.writer(inactiveUserWriter())
				.listener(inativeStepListener)
				.listener(inactiveChunkListener)
				.build();
	}

    @Bean
    @StepScope
    public QueueItemReader<User> inactiveUserReader() {
        List<User> oldUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE);
        return new QueueItemReader<>(oldUsers);
    }

    public ItemProcessor<User, User> inactiveUserProcessor() {
        return User::setInactive;
    }

    public ItemWriter<User> inactiveUserWriter() {
        return ((List<? extends User> users) -> userRepository.saveAll(users));
    }
}
