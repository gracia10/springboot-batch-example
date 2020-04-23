package com.community.batch;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.community.batch.domain.enums.UserStatus;
import com.community.batch.repository.UserRepository;

import lombok.extern.slf4j.Slf4j;
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
@Slf4j
public class InactiveUserJobTest {
	
	@Autowired
	private JobLauncherTestUtils jobLauncherTestUtils;
	
	@Autowired
	private UserRepository userRepository;
	
	@Test
	public void 전환_테스트() throws Exception{
		JobExecution jobExecution = jobLauncherTestUtils.launchJob(new JobParametersBuilder().addDate("nowDate", new Date()).toJobParameters());
		
		assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
		assertEquals(0, userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE).size());
	}
}
