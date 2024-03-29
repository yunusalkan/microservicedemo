package com.microservices.demo.twitter.to.kafka.service.runner.impl;


import com.microservices.demo.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;

import org.springframework.stereotype.Component;
import twitter4j.TwitterException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-v2-tweets} && not ${twitter-to-kafka-service.enable-mock-tweets}")
@Slf4j
@RequiredArgsConstructor
public class TwitterV2KafkaStreamRunner implements StreamRunner {

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
    private final TwitterV2StreamHelper twitterV2StreamHelper;

    @Override
    public void start() throws TwitterException {
        String bearerToken = twitterToKafkaServiceConfigData.getTwitterV2BearerToken();
        if (Objects.nonNull(bearerToken)) {
            try{
                twitterV2StreamHelper.setupRules(bearerToken, getRules());
                twitterV2StreamHelper.connectStream(bearerToken);
            }
            catch (IOException | URISyntaxException e) {
                log.error("Error streaming tweets!", e);
                throw new RuntimeException(e);
            }
        }
        else {
            log.error("There was a problem getting your bearer token. " +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
            throw new RuntimeException("There was a problem getting your bearer token. " +
                    "Please make sure you set the TWITTER_BEARER_TOKEN environment variable");
        }
    }

    private Map<String, String> getRules() {
        List<String> keywords = twitterToKafkaServiceConfigData.getTwitterKeywords();
        Map<String, String> rules = keywords.stream().collect(Collectors.toMap(String::toString, k -> "Keyword: " + k));
        log.info("Created filter for twitter stream for keywords: {} ", keywords);
        return rules;
    }
}
