package com.zeotap.logging;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

public class LogPullerTask implements StreamTask {

	public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
		String event = new String ((byte[]) envelope.getMessage());
		System.out.println(event);
	}

}
