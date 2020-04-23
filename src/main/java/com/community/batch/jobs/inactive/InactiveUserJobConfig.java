package com.community.batch.jobs.inactive;

import java.util.stream.IntStream;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class InactiveUserJobConfig {

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	
	@Bean
	public TaskExecutor taskExecutor() {
		return new SimpleAsyncTaskExecutor("Batch_Task");
	}
	
	@Bean 
	public Job inactiveUserJob(Step inactiveUserTaskletStep) { 
		return jobBuilderFactory.get("inactiveUserJob")
				.preventRestart()
				//.start(inactiveUserFlow(inactiveUserTaskletStep))
				.start(multiFlow(inactiveUserTaskletStep))
				.end()
				.build();
	}
	
	//Tasklet
	@Bean
	public Step inactiveUserTaskletStep(InactiveItemTasklet inactiveItemTasklet) { 
		return stepBuilderFactory.get("inactiveUserTaskletStep")
				.tasklet(inactiveItemTasklet)
				.build(); 
	}

	@Bean
	public Flow multiFlow(Step inactiveUserTaskletStep) {
		Flow flows[] = new Flow[5];
		IntStream.range(0, flows.length).forEach(i -> flows[i] = new FlowBuilder<Flow>("MultiFlow"+i).from(inactiveUserFlow(inactiveUserTaskletStep)).end());
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("MutiFlowTest");
		return flowBuilder
				.split(taskExecutor())
				.add(flows)
				.build();
	}
	
	//빈으로 생성하면 싱글톤이기 때문에 멀티 Flow를 위해서는 빈으로 등록하면 안됨
	public Flow inactiveUserFlow(Step inactiveUserTaskletStep) {
		FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("inactiveUserFlow");
		return flowBuilder
				.start(new InactiveJobExecutionDecider())
				.on(FlowExecutionStatus.FAILED.getName()).end()
				.on(FlowExecutionStatus.COMPLETED.getName()).to(inactiveUserTaskletStep)
				.end();
	}
	
}
