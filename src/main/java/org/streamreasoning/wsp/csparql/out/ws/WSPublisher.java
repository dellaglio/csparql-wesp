package org.streamreasoning.wsp.csparql.out.ws;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

@WebSocket
public class WSPublisher {
	private WSQueryObserver observer;
	private Session session;
	
	public WSPublisher(WSQueryObserver observer) {
		this.observer = observer;
	}

	@OnWebSocketConnect
	public void onConnect(Session session){
		this.session = session;
		observer.add(this);
	}
	
	@OnWebSocketClose
	public void onClose(int statusCode, String reason){
		observer.remove(this);
	}

	public Session getSession() {
		return session;
	}
	
}
