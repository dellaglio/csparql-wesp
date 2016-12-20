package org.streamreasoning.wsp.csparql;

import java.text.ParseException;

import org.streamreasoning.wsp.csparql.out.mqtt.MQTTObserver;

import eu.larkc.csparql.common.utils.ReasonerChainingType;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;

public class CsparqlEngineSD extends CsparqlEngineImpl{
	public CsparqlEngineSD() {
		super();
		new ServiceDescriptor(this);
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference);
		c.addObserver(new MQTTObserver(c.getName()));
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String rulesFileSerialization, ReasonerChainingType chainingType, String tBoxFileSerialization)
			throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, rulesFileSerialization, chainingType, tBoxFileSerialization);
		c.addObserver(new MQTTObserver(c.getName()));
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String tBoxFileSerialization) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, tBoxFileSerialization);
		c.addObserver(new MQTTObserver(c.getName()));
		return c;
	}
	
	@Override
	public CsparqlQueryResultProxy registerQuery(String command, boolean activateInference,
			String rulesFileSerialization, ReasonerChainingType chainingType) throws ParseException {
		CsparqlQueryResultProxy c = super.registerQuery(command, activateInference, rulesFileSerialization, chainingType);
		c.addObserver(new MQTTObserver(c.getName()));
		return c;
	}
	
	
}
