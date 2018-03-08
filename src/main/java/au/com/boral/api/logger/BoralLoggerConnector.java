package au.com.boral.api.logger;

import java.util.Date;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.mule.api.MuleEvent;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.param.Default;
import org.mule.api.annotations.param.Optional;

import au.com.boral.api.util.DateTimeUtil;

@Connector(name = "boral-logger", friendlyName = "BoralLogger")
public class BoralLoggerConnector {

	private static final String X_BORAL_CID = "X-Boral-CID";
	private static final String X_BORAL_MESSAGE_ID = "X-Boral-MessageID";
	private static final String X_START_TIME = "X-Start-Time";
	private static final String X_DOWNSTREAM_START_TIME = "X-Downstream-Start-Time";
	private static final String X_BORAL_FLOW_START_TIME = "X-Boral-FlowStartTime";
	private static final String SERVICE = "Service";


	/**
	 * Custom processor The MuleEvent is inject from the mule context during
	 * runtime.
	 */

	// Initialize the logger
	private static Logger logger = Logger.getLogger("au.com.boral.api.logger");
	private static Logger batchLogger = Logger.getLogger("au.com.boral.api.BatchLogger");

	public enum LoggerName {
		BATCH, SIMPLE
	};

	public enum LogLevel {
		INFO, DEBUG, ERROR, FATAL, WARN
	};

	public enum RequestDirectionType {
		REQUEST_IN, REQUEST_OUT
	};

	public enum ResponseDirectionType {
		RESPONSE_IN, RESPONSE_OUT
	};

	public enum MetricType {
		SERVICE_TIME, DOWNSTREAM_TIME
	};

	/**
	 * @param event - source of logging information
	 * @param direction - IN - incoming request
	 *                    OUT - outgoing request
	 * @param serviceName - name of service
	 * @param payload - request payload to log
	 * @param level - logging level
	 * @param logRequestPayload - indicator if payload should be logged. if true payload will be logged, 
	 *                            otherwise only internal variables will be initiated.
	 *                            This flag is useful when logging request is not required but metrics are 
	 *                            needed.  
	 * @param loggerName - SIMPLE or BATCH, batch logger to be used for high volume logging
	 */

	@Processor
	public void requestLog(
			MuleEvent event, 
			RequestDirectionType direction, 
			@Optional String serviceName, 
			@Optional String payload, 
			@Default(value = "INFO") LogLevel level,
			@Default(value = "true") boolean logRequestPayload,
			@Default(value = "SIMPLE") LoggerName loggerName
			) { 

		injector(event);

		//
		// note that properties are passed over http so start time will be passed from
		// calling service. We need to reset it so current service metrics will be calculated
		//
		if (direction == RequestDirectionType.REQUEST_OUT) {
			event.getMessage().setInvocationProperty(X_DOWNSTREAM_START_TIME, System.currentTimeMillis());
		} else {
			event.getMessage().setInvocationProperty(X_START_TIME, System.currentTimeMillis());
			event.getMessage().setInvocationProperty(X_BORAL_MESSAGE_ID, event.getId());
		}

		//if (serviceName != null && !serviceName.isEmpty()) {
		//	event.getMessage().setInvocationProperty(SERVICE, serviceName);
		//}
		
		if (logRequestPayload) {
			JSONObject msg = prepareCommonEntries(event, serviceName);
			msg.put("Type", "Payload");
			msg.put("Direction", direction);
			msg.put("Message", StringUtils.isNotBlank(payload)? payload: getPayload(event));
	
			log(level, msg.toString(), loggerName);
		}
	}

	@Processor
	public void eventLog(
			MuleEvent event, 
			String content, 
			@Default(value = "INFO") LogLevel level,
	        @Default(value = "false") boolean emit,
			@Default(value = "SIMPLE") LoggerName loggerName) {

		BoralLoggerConnector.injector(event);

		JSONObject msg = prepareCommonEntries(event, null);
	
		JSONTokener tokener = new JSONTokener(content);
		
		JSONObject boralEvent = new JSONObject(tokener);
		for(String key : JSONObject.getNames(boralEvent)) {
		    msg.put(key, boralEvent.get(key));
		}
		
		msg.put("Type", "Event");

		log(level, msg.toString(), loggerName);
		if (emit) {
			log(level, "Not Event mechanism implemented", loggerName);
		}
	}

	@Processor
	public void healthLog(
			MuleEvent event, 
			String content, 
			@Default(value = "INFO") LogLevel level,
			@Default(value = "SIMPLE") LoggerName loggerName) {

		BoralLoggerConnector.injector(event);

		JSONObject msg = prepareCommonEntries(event, null);
	
		JSONTokener tokener = new JSONTokener(content);
		
		JSONObject boralEvent = new JSONObject(tokener);
		for(String key : JSONObject.getNames(boralEvent)) {
		    msg.put(key, boralEvent.get(key));
		}
		
		msg.put("Type", "Health");

		log(level, msg.toString(), loggerName);
	}

	/**
	 * @param event
	 * 
	 *            Metric log is used at the end of the flow for finding the
	 *            execution time.
	 */

	@Processor
	public void metricLog(
					MuleEvent event, 
					MetricType direction, 
					@Optional String serviceName, 
					@Default(value = "INFO") LogLevel level,
					@Default(value = "SIMPLE") LoggerName loggerName) {

		long duration = 0L;
		long starttime = 0l;
		
		//added for testing overriding of x-boral-cid
		BoralLoggerConnector.injector(event);

		if (event != null && event.getMessage() != null) {
		
			if (direction == MetricType.DOWNSTREAM_TIME) { 
				if (event.getMessage().getInvocationProperty(X_DOWNSTREAM_START_TIME) != null) {
				    starttime = event.getMessage().getInvocationProperty(X_DOWNSTREAM_START_TIME);
				}
			} else { 
		        if (event.getMessage().getInvocationProperty(X_START_TIME) != null) {
		            
		        	if (event.getMessage().getInvocationProperty(X_START_TIME) instanceof Long)	
				        starttime = event.getMessage().getInvocationProperty(X_START_TIME);
		            
		            else if (event.getMessage().getInvocationProperty(X_START_TIME) instanceof String) {
		            	try {
				            starttime = Long.valueOf((String)event.getMessage().getInvocationProperty(X_START_TIME));
		            	} catch (NumberFormatException e) {
		            		// TODO: trace the origins if non Long setter for start time
		            	}
		            }
  	            }
		    }
		}

		if (starttime != 0l) {
			duration = System.currentTimeMillis() - starttime;
		}

		JSONObject msg = prepareCommonEntries(event, serviceName);
		msg.put("Type", "Metric");
		msg.put("MetricType", direction);
		msg.put("ExecutionTimeMs", duration);

		log(level, msg.toString(), loggerName);
	}

	/**
	 * @param event Payload log is used for logging the incoming payload
	 */
	@Processor
	public void responseLog(
					MuleEvent event, 
					ResponseDirectionType direction, 
					@Optional String serviceName, 
					@Optional String payload, 
					@Default(value = "INFO") LogLevel level,
					@Default(value = "SIMPLE") LoggerName loggerName) {
        
		//added for testing overriding of x-boral-cid
		BoralLoggerConnector.injector(event);
		
		JSONObject msg = prepareCommonEntries(event, serviceName);
		msg.put("Type", "Payload");
		msg.put("Direction", direction);
		msg.put("Message", StringUtils.isNotBlank(payload)? payload: getPayload(event));

		log(level, msg.toString(), loggerName);
		
		//
		// additionally calling metrics
		//
		if (direction == ResponseDirectionType.RESPONSE_OUT) {
			metricLog(event, MetricType.SERVICE_TIME, serviceName, level, loggerName);
		} else {
			metricLog(event, MetricType.DOWNSTREAM_TIME, serviceName, level, loggerName);
		}
	}
		

	/**
	 * @param event Error log is used for logging the error details during
	 *              exception.
	 */
	@Processor
	public void errorLog(
				MuleEvent event, 
				String content, 
				LogLevel level,
				@Default(value = "SIMPLE") LoggerName loggerName) {

		injector(event);

		String error = content;

		if (event != null && 
			event.getMessage() != null && 
			event.getMessage().getExceptionPayload() != null && 
			event.getMessage().getExceptionPayload().getRootException() != null &&
			event.getMessage().getExceptionPayload().getRootException().getMessage() != null) {

			error = event.getMessage().getExceptionPayload().getRootException().getMessage();

		} else if (event != null && 
				event.getMessage() != null && 
				event.getMessage().getExceptionPayload() != null &&
				event.getMessage().getExceptionPayload().getMessage() != null) {
			
			error = event.getMessage().getExceptionPayload().getMessage();
			
		} else {
			// nothing else, error is defaulted to content
		}

		JSONObject msg = prepareCommonEntries(event, null);
		msg.put("Type", "Error");
		msg.put("Error", error);
		msg.put("Additional", content);

		log(level, msg.toString(), loggerName);
	}

	/**
	 * 
	 * @param event
	 * @param content
	 * 
	 *            Custom log is used for logging custom details or data in mule
	 *            flow
	 * 
	 */
	@Processor
	public void customLog(
					MuleEvent event, 
					String content, 
					@Default(value = "INFO") LogLevel level,
					@Default(value = "SIMPLE") LoggerName loggerName) {
		injector(event);
		
		JSONObject msg = prepareCommonEntries(event, null);
		msg.put("Type", "Custom");
		msg.put("Message", content);
		msg.put("Level", level.toString());

		log(level, msg.toString(), loggerName);
	}

	/**
	 * 
	 * @param event
	 * 
	 *            This method is used for injecting the inbound property at
	 *            start of the flow
	 */
	public static void injector(MuleEvent event) {

		// Verifying whether logger point is just after AMQ connector
		if (event.getMessage().getInboundProperty("properties") != null) {
			amqInjector(event);

		} else {

			if (event.getMessage().getInvocationProperty(X_BORAL_MESSAGE_ID) == null) {

				if (event.getMessage().getInboundProperty(X_BORAL_MESSAGE_ID) != null) {
					event.getMessage().setInvocationProperty(X_BORAL_MESSAGE_ID,
							event.getMessage().getInboundProperty(X_BORAL_MESSAGE_ID));
				} else {
					event.getMessage().setInvocationProperty(X_BORAL_MESSAGE_ID, event.getId());
				}
			}
			
			if (event.getMessage().getInvocationProperty(X_START_TIME) == null) {
				if (event.getMessage().getInboundProperty(X_START_TIME) != null) {
					event.getMessage().setInvocationProperty(X_START_TIME,
							event.getMessage().getInboundProperty(X_START_TIME));
				} else {
					event.getMessage().setInvocationProperty(X_START_TIME, System.currentTimeMillis());
				}
			}

			event.getMessage().setInvocationProperty(X_BORAL_FLOW_START_TIME, System.currentTimeMillis());

			if (event.getMessage().getInboundProperty("btxnid") != null
					&& event.getMessage().getInboundProperty(X_BORAL_CID) == null) {

				event.getMessage().setInvocationProperty(X_BORAL_CID, event.getMessage().getInboundProperty("btxnid"));

			} else {
				if (event.getMessage().getInboundProperty(X_BORAL_CID) != null ) {

					event.getMessage().setInvocationProperty(X_BORAL_CID,
							event.getMessage().getInboundProperty(X_BORAL_CID));

				} else if (event.getMessage().getInvocationProperty(X_BORAL_CID) != null) {
					//do nothing
				}else {
					event.getMessage().setInvocationProperty(X_BORAL_CID,
							event.getMessage().getInvocationProperty(X_BORAL_MESSAGE_ID));
				}
			}
		}
	}

	/**
	 * @param event
	 * 
	 *            This menthod is used for injecting the inbound property incase
	 *            of AMQ protocol
	 */

	public static void amqInjector(MuleEvent event) {
		HashMap<?, ?> properties = event.getMessage().getInboundProperty("properties");
		
		if (event.getMessage().getInvocationProperty(X_BORAL_MESSAGE_ID) == null) {
			if (properties.get(X_BORAL_MESSAGE_ID) != null) {
				event.getMessage()
				     .setInvocationProperty(X_BORAL_MESSAGE_ID, 
					                        properties.get(X_BORAL_MESSAGE_ID));
			} else {
				event.getMessage().setInvocationProperty(X_BORAL_MESSAGE_ID, event.getId());
			}
		}

		if (event.getMessage().getInvocationProperty(X_START_TIME) == null) {
			if (event.getMessage().getInboundProperty(X_START_TIME) != null) {
				event.getMessage().setInvocationProperty(X_START_TIME, event.getMessage().getInboundProperty(X_START_TIME));
			} else {
				event.getMessage().setInvocationProperty(X_START_TIME, System.currentTimeMillis());
			}
		}

		event.getMessage().setInvocationProperty(X_BORAL_FLOW_START_TIME, System.currentTimeMillis());

		if (event.getMessage().getInboundProperty("btxnid") != null
				&& event.getMessage().getInboundProperty(X_BORAL_CID) == null) {

			event.getMessage()
			     .setInvocationProperty(X_BORAL_CID, 
			    		                event.getMessage().getInboundProperty("btxnid"));

		} else {
			if (event.getMessage().getInboundProperty(X_BORAL_CID) != null) {

				event.getMessage()
				     .setInvocationProperty(X_BORAL_CID, 
						                    properties.get(X_BORAL_CID).toString());
			
			} else if (properties.get(X_BORAL_CID) != null) {

					event.getMessage()
					     .setInvocationProperty(X_BORAL_CID, 
							                    properties.get(X_BORAL_CID).toString());

			} else {
				event.getMessage()
				     .setInvocationProperty(X_BORAL_CID,
					                        event.getMessage()
					                             .getInvocationProperty(X_BORAL_MESSAGE_ID));
			}
		}
	}

	/**
	 * 
	 * @param event
	 * 
	 *            This method is used for the crossboundary injection of
	 *            property
	 */
	@Processor
	public void crossBoundayInjector(MuleEvent event) {

		if (event == null || event.getMessage() == null) {
			return;
		}

		event.getMessage()
		     .setOutboundProperty(X_BORAL_CID, event.getMessage().getInvocationProperty(X_BORAL_CID));
	}

	private JSONObject prepareCommonEntries(MuleEvent event, String service) {

		String messageId = "";
		String boralCID = "";
		String flowName = "";
		String application = "";

		if (event != null) {
			if (event.getMessage() != null) {
				messageId = event.getMessage().getInvocationProperty(X_BORAL_MESSAGE_ID);
				boralCID = event.getMessage().getInvocationProperty(X_BORAL_CID);
			}

			if (event.getFlowConstruct() != null) {
				flowName = event.getFlowConstruct().getName();
			}

			if (event.getMuleContext() != null && event.getMuleContext().getConfiguration() != null) {
				application = event.getMuleContext().getConfiguration().getId();
			}
		}
		String environment = System.getProperty("mule.env");

		JSONObject msg = new JSONObject();
		msg.put("Application", application);
		msg.put("Env", environment);
		msg.put("X-Boral-Timestamp", DateTimeUtil.toString(new Date()));
		msg.put(X_BORAL_CID, boralCID);
		msg.put(X_BORAL_MESSAGE_ID, messageId);
		msg.put("X-Boral-FlowName", flowName);
		msg.put(SERVICE, service == null? flowName: service);

		return msg;
	}

	private void log(LogLevel level, String message, LoggerName loggerName) {
		if (loggerName == LoggerName.BATCH) {
			switch(level) {
			    case DEBUG: batchLogger.debug(message);
	                        break;
			    case INFO: batchLogger.info(message);
	                       break;
			    case WARN: batchLogger.warn(message);
	                       break;
			    case ERROR: batchLogger.error(message);
	                        break;
			    case FATAL: batchLogger.fatal(message);
	                        break;
	            default: batchLogger.warn("Unknown level: " + level.toString());
	            		 batchLogger.info(message);
			}
		} else {
			switch(level) {
			    case DEBUG: logger.debug(message);
	                        break;
			    case INFO: logger.info(message);
	                       break;
			    case WARN: logger.warn(message);
	                       break;
			    case ERROR: logger.error(message);
	                        break;
			    case FATAL: logger.fatal(message);
	                        break;
	            default: logger.warn("Unknown level: " + level.toString());
	                     logger.info(message);
			}
		}  
	}

	private String getPayload(MuleEvent event) {

		try {
			return event.getMessage().getPayloadAsString();
		} catch (Exception e) {
			return "Unable to get payload: " + e.getClass().getSimpleName() + " " + e.getMessage();
		}
	}
}
