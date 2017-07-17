package org.streamreasoning.wsp.csparql.out.ws;

import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;

public class WSQueryCreator implements WebSocketCreator {
	private WSQueryObserver observer;
	
	public WSQueryCreator(WSQueryObserver observer) {
		this.observer = observer;
	}

	@Override
	public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) {
		return new WSPublisher(observer);
	}

}
