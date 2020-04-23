package com.community.batch.jobs.inactive.listener;

import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class InativeStepListener{

	@BeforeStep
	public void beforeStep() throws Exception {
		log.info("Before Step");
	}

	@AfterStep
	public void afterStep() throws Exception {
		log.info("After Step");
	}

}
