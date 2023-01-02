package com.mas2022datascience.tracabgen5enricher.admin;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.stereotype.Component;

@Component
public class Topics {

  @Value(value = "${topic.tracab-01.name}")
  private String topicName1;
  @Value(value = "${topic.tracab-01.partitions}")
  private Integer topicPartitions1;
  @Value(value = "${topic.tracab-01.replication-factor}")
  private Integer topicReplicationFactor1;

  // creates or alters the topic
  @Bean
  public NewTopic tracab01() {
    return TopicBuilder.name(topicName1)
        .partitions(topicPartitions1)
        .replicas(topicReplicationFactor1)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }

  @Value(value = "${topic.tracab-02.name}")
  private String topicName2;
  @Value(value = "${topic.tracab-02.partitions}")
  private Integer topicPartitions2;
  @Value(value = "${topic.tracab-02.replication-factor}")
  private Integer topicReplicationFactor2;

  // creates or alters the topic
  @Bean
  public NewTopic tracab02() {
    return TopicBuilder.name(topicName2)
        .partitions(topicPartitions2)
        .replicas(topicReplicationFactor2)
        .config(TopicConfig.RETENTION_MS_CONFIG, "-1")
        .build();
  }
}