package es.unizar.tmdad.lab2.configuration;

import java.util.ArrayList;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.social.twitter.api.StreamListener;
import org.springframework.social.twitter.api.Tweet;

import es.unizar.tmdad.lab2.domain.MyTweet;
import es.unizar.tmdad.lab2.domain.TargetedTweet;
import es.unizar.tmdad.lab2.service.TwitterLookupService;

@Configuration
@EnableIntegration
@IntegrationComponentScan
@ComponentScan
public class TwitterFlow {

	@Autowired
	private TwitterLookupService lookupService;
	
	@Bean
	public DirectChannel requestChannel() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow sendTweet() {
		return IntegrationFlows.from(requestChannel())
				.filter((Object t) -> t instanceof Tweet)
				.<Tweet,TargetedTweet>transform(t -> new TargetedTweet(new MyTweet(t), lookupService.getQueries().stream().
						filter(topic -> t.getUnmodifiedText().contains(topic)).collect(Collectors.toList())))
				.split(TargetedTweet.class, t -> {
					ArrayList<TargetedTweet> listTargetedTweets = new ArrayList<TargetedTweet>();
					for(String s : t.getTargets()) {
						listTargetedTweets.add(new TargetedTweet(t.getTweet(), s));
					}
					return listTargetedTweets;
				})
				.<TargetedTweet,TargetedTweet>transform(t -> {
					MyTweet m = t.getTweet();
					String tar = t.getFirstTarget();
					m.setUnmodifiedText(m.getUnmodifiedText().replaceAll(tar, "<b>"+tar+"</b>"));
					return t;
				})
				.handle("streamSendingService", "sendTweet")
				.get();
	}

}

@MessagingGateway(name = "integrationStreamListener", defaultRequestChannel = "requestChannel")
interface MyStreamListener extends StreamListener {

}
