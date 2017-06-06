package edu.mit.streamjit.test.apps.twitter;

import java.util.List;
import java.util.Scanner;

import twitter4j.DirectMessage;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterConnection {


          public static void main(String[] args) {

                         ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.setDebugEnabled(true)
      .setOAuthConsumerKey("A0qa2gDQTQIkVLaQwrgPy7o8X")
      .setOAuthConsumerSecret("FUflLLqJxCmlqrP1ev5ihnHq6PpK3eWDm6ljm4gEQjcGoPYQPr")
      .setOAuthAccessToken("2454581378-lEaaFQCFRbNKp5iM9K0qqDQj4FEcKsraudrFNQO")
      .setOAuthAccessTokenSecret("pmK3TdvszfRt0OsDwFwqSycRvIHHA4Gj5EM8r4Fz44f3r");
      TwitterFactory tf = new TwitterFactory(cb.build());
      Twitter twitter = tf.getInstance();
      try {
    	  List<DirectMessage> messages = twitter.getDirectMessages();
          for (DirectMessage message : messages) 
          {
              System.out.println(message.getText());
          }
          System.exit(0);
      } catch (TwitterException te) {
          te.printStackTrace();
          System.out.println("Failed to send a direct message: " + te.getMessage());
          System.exit(-1);
      }
              }
 
}