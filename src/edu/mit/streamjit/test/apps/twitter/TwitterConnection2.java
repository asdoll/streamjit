package edu.mit.streamjit.test.apps.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.BasicAuth;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterConnection2 {

  public static void run() throws InterruptedException {

    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);


    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    List<Long> followings = Lists.newArrayList(1234L, 566788L);
    List<String> terms = Lists.newArrayList("twitter", "api");
    endpoint.followings(followings);
    endpoint.trackTerms(terms);

    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1("A0qa2gDQTQIkVLaQwrgPy7o8X", "FUflLLqJxCmlqrP1ev5ihnHq6PpK3eWDm6ljm4gEQjcGoPYQPr", "2454581378-lEaaFQCFRbNKp5iM9K0qqDQj4FEcKsraudrFNQO", "pmK3TdvszfRt0OsDwFwqSycRvIHHA4Gj5EM8r4Fz44f3r");

    BasicClient client = new ClientBuilder()
            .name("sampleExampleClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();


    client.connect();


    for (int msgRead = 0; msgRead < 1000; msgRead++) {
      if (client.isDone()) {
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }

      String msg = queue.poll(5, TimeUnit.SECONDS);
      if (msg == null) {
        System.out.println("Did not receive a message in 5 seconds");
      } else {
        System.out.println(msg);
      }
    }

    client.stop();

    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
  }

  public static void main(String[] args) {
    try {
    	TwitterConnection2.run();
    } catch (InterruptedException e) {
      System.out.println(e);
    }
    
  }
}