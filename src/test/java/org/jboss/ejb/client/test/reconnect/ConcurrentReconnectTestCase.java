package org.jboss.ejb.client.test.reconnect;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.jboss.ejb.client.ContextSelector;
import org.jboss.ejb.client.EJBClient;
import org.jboss.ejb.client.EJBClientConfiguration;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.ejb.client.PropertiesBasedEJBClientConfiguration;
import org.jboss.ejb.client.StatelessEJBLocator;
import org.jboss.ejb.client.remoting.ConfigBasedEJBClientContextSelector;
import org.jboss.ejb.client.test.common.DummyServer;
import org.jboss.logging.Logger;
import org.junit.Assert;
import org.junit.Test;

public class ConcurrentReconnectTestCase {

    private static final Logger logger = Logger.getLogger(ConcurrentReconnectTestCase.class);

    /**
     * Tests that a re-connection to a server, which went down after it was initially connected,
     * is successful under concurrent invocation attempts.
     *
     * @throws Exception
     */
    @Test
    public void testConcurrentReconnectOfBrokenConnection() throws Exception {
        ContextSelector<EJBClientContext> oldEJBClientContextSelector = null;
        DummyServer server = null;
        final String ejbClientConfigResource = "reconnect-jboss-ejb-client.properties";
        final InputStream propertiesStream = this.getClass().getClassLoader().getResourceAsStream(ejbClientConfigResource);
        final int NUM_CONCURRENT_REQUESTS = 3;
        try {
            Assert.assertNotNull("Could not find " + ejbClientConfigResource + " through classloader", propertiesStream);
            // start the server and register the deployment
            server = this.startServer();
            server.register("my-app", "my-module", "", EchoBean.class.getSimpleName(), new EchoBean());
            logger.info("Started server");

            // load the ejb client properties
            final Properties ejbClientProperties = new Properties();
            ejbClientProperties.load(propertiesStream);

            // create a configuration out of the properites
            final EJBClientConfiguration ejbClientConfiguration = new PropertiesBasedEJBClientConfiguration(ejbClientProperties);
            // create and set the selector
            oldEJBClientContextSelector = EJBClientContext.setSelector(new ConfigBasedEJBClientContextSelector(ejbClientConfiguration));

            // create a proxy for invocation
            final StatelessEJBLocator<Echo> statelessEJBLocator = new StatelessEJBLocator<Echo>(Echo.class, "my-app", "my-module", EchoBean.class.getSimpleName(), "");
            final Echo proxy = EJBClient.createProxy(statelessEJBLocator);
            Assert.assertNotNull("Received a null proxy", proxy);

            // try invoking
            final String message = "Yet another Hello World!!!";
            // should succeed since the server is up
            final String firstEcho = proxy.echo(message);
            Assert.assertEquals("Unexpected echo message returned by the bean", firstEcho, message);

            // now stop the server
            server.stop();
            server = null;
            logger.info("Stopped server");

            // now invoke on the proxy and this should fail since the server is down
            try {
                final String echo = proxy.echo(message);
                Assert.fail("Invocation was expected to fail since the server has been stopped");
            } catch (IllegalStateException ise) {
                // expected
                logger.info("Got the expected failure during invocation on proxy, due to server being down", ise);
            }

            // now restart server and register the deployment
            server = this.startServer();
            server.register("my-app", "my-module", "", EchoBean.class.getSimpleName(), new EchoBean());
            logger.info("Re-started server");

            // now invoke on the proxy. This should succeed since the reconnect logic should now reconnect to the
            // restarted server
            final ExecutorService executorService = Executors.newFixedThreadPool(NUM_CONCURRENT_REQUESTS);
            final Future[] results = new Future[NUM_CONCURRENT_REQUESTS];
            for (int i = 0; i < NUM_CONCURRENT_REQUESTS; i++) {
                final int requestId = i + 1;
                final String requestIdString = requestId + " / " + NUM_CONCURRENT_REQUESTS;
                results[i] = executorService.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                logger.info("Sending invocation: " + requestIdString);
                                final String echo = proxy.echo(message);
                                Assert.assertEquals("Got an unexpected echo", echo, message);
                                logger.info("Got expected invocation result: " + requestIdString);
                            } catch (RuntimeException e) {
                                logger.info("Got exception from request " + requestIdString + ": " + e.getMessage());
                                throw e;
                            }
                        }
                    });
            }
            // wait for the threads to complete
            for (int i = 0; i < NUM_CONCURRENT_REQUESTS;i++) {
                results[i].get(10, TimeUnit.SECONDS);
            }
            executorService.shutdown();
        } finally {
            if (server != null) {
                try {
                    server.stop();
                    logger.info("Stopped server");
                } catch (Exception e) {
                    // ignore
                    logger.debug("Ignoring exception during server shutdown", e);
                }
            }
            if (propertiesStream != null) {
                propertiesStream.close();
            }
            if (oldEJBClientContextSelector != null) {
                EJBClientContext.setSelector(oldEJBClientContextSelector);
            }
        }
    }

    /**
     * Starts and returns the server
     *
     * @return
     * @throws IOException
     */
    private DummyServer startServer() throws IOException {
        final DummyServer server = new DummyServer("localhost", 6999);
        server.start();
        return server;
    }
}
