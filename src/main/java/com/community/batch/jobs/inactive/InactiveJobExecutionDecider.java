package com.community.batch.jobs.inactive;

import java.util.Random;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

import lombok.extern.slf4j.Slf4j;

/*
 * 양수일 경우 실행. 음수일 경우 어떤 행동도 취하지 않음
*/

@Slf4j
public class InactiveJobExecutionDecider implements JobExecutionDecider{

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		if(new Random().nextInt() > 0) {
			log.info("FlowExecutionStatus.COMPLETED");
			return FlowExecutionStatus.COMPLETED;
		}
		log.info("FlowExecutionStatus.FAILED");
		return FlowExecutionStatus.FAILED;
	}

}
