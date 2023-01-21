package com.mas2022datascience.tracabgen5enricher.processor;

import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.TracabGen5TF01;
import com.mas2022datascience.util.Time;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;

  @Value(value = "${topic.tracab-01.name}")
  private String topicIn;

  @Value(value = "${topic.tracab-02.name}")
  private String topicOut;

  private static double VELOCITY_MAXIMAL_VALUE_STATIC;

  @Value("${maxima.velocity}")
  public void setNameStatic(double velocityMaximalValue){
    KafkaStreamsRunnerDSL.VELOCITY_MAXIMAL_VALUE_STATIC = velocityMaximalValue;
  }

  @Bean
  public KStream<String, TracabGen5TF01> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);
    final Serde<TracabGen5TF01> tracabGen5TF01Serde = new SpecificAvroSerde<>();
    tracabGen5TF01Serde.configure(serdeConfig, false); // `false` for record values

    KStream<String, TracabGen5TF01> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), tracabGen5TF01Serde));

    final StoreBuilder<KeyValueStore<String, TracabGen5TF01>> myStateStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("MyTracabGen5StateStore"),
            Serdes.String(), tracabGen5TF01Serde);
    kStreamBuilder.addStateStore(myStateStore);

    final MyTracabGen5StateHandler myStateHandler =
        new MyTracabGen5StateHandler(myStateStore.name());

    // invoke the transformer
    KStream<String, TracabGen5TF01> transformedStream = stream
        .transform(() -> myStateHandler, myStateStore.name());

    // peek into the stream and execute a println
    //transformedStream.peek((k,v) -> System.out.println("key: " + k + " - value:" + v));

    // publish result
    transformedStream.to(topicOut);

    return stream;
  }

  private static final class MyTracabGen5StateHandler implements
      Transformer<String, TracabGen5TF01, KeyValue<String, TracabGen5TF01>> {
    final private String storeName;
    private KeyValueStore<String, TracabGen5TF01> stateStore;

    public MyTracabGen5StateHandler(final String storeName) {
      this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext processorContext) {
      stateStore = processorContext.getStateStore(storeName);
    }

    @Override
    public KeyValue<String, TracabGen5TF01> transform(String key,
        TracabGen5TF01 value) {
      try {
        if (stateStore.get(key) == null) {
          stateStore.put(key, value);
          return new KeyValue<>(key, stateStore.get(key));
        }
      } catch (org.apache.kafka.common.errors.SerializationException ex) {
        // the first time the state store is empty, so we get a serialization exception
        stateStore.put(key, value);
        return new KeyValue<>(key, stateStore.get(key));
      } catch (Exception e) {
        e.printStackTrace();
        return null;
      }

      TracabGen5TF01 oldFrame = stateStore.get(key);
      HashMap<String, Object> oldObjectsMap = new HashMap<>();
      oldFrame.getObjects().forEach( object ->
          oldObjectsMap.put(object.getId(), object)
      );

      List<Object> actualObjects = value.getObjects();
      // update acceleration and distance in actual object
      actualObjects.forEach( actualObject -> {
        if (oldObjectsMap.containsKey(actualObject.getId())) {
         actualObject.setAccelleration(calcAcceleration(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getId()),
              oldFrame.getUtc()).orElse(null));
          actualObject.setDistance(getEuclidianDistance(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getId()),
              oldFrame.getUtc()).orElse(null));
        }
      });

      stateStore.put(key, value);
      return new KeyValue<>(key, stateStore.get(key));

    }

    @Override
    public void close() { }
  }

  /**
   * calculates the time difference in seconds
   * @param actualUtc UTC time as a String
   * @param oldUtc UTC time as a String
   * @return time difference in seconds
   */
  private static float getTimeDifference(String actualUtc, String oldUtc) {
    // represents the divisor that is needed to get s. Ex. ms to s means 1000 as 1000ms is 1s
    float timeUnitDivisor = 1000;
    return (Time.utcString2epocMs(actualUtc) - Time.utcString2epocMs(oldUtc))/timeUnitDivisor;
  }

  /**
   * calculates the euclidian distance [m] between two points in a 3 dimensional space
   *
   * @param actualObject  of type Object
   *      <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject     of type Object
   *      <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return distance     of type double in meters
   *                      between 2 points in a 3 dimensional space.
   */
  private static Optional<Double> getEuclidianDistance(Object actualObject, String actualUtc,
      Object oldObject, String oldUtc) {
    // represents the divisor that is needed to get m. Ex. cm to m means 100 as 100cm is 1m
    int distanceUnitDivisor = 100;

    double euclidianDistance = Math.sqrt(
        Math.pow(oldObject.getX()-actualObject.getX(), 2)
            + Math.pow(oldObject.getY()-actualObject.getY(), 2)
            + Math.pow(oldObject.getZ()-actualObject.getZ(), 2)
    ) / distanceUnitDivisor;
    double timeDifference = getTimeDifference(actualUtc, oldUtc);

    // if the distance is higher than 0, then an empty optional is returned
    // v_max= velocity world record holder Arjen Robben mit 37 km/h (10.277777778m/s)
    // s_max=v_max*timedifference=10,2m/s∗0.04s= 0.408m = 40.8cm
    if (euclidianDistance < (VELOCITY_MAXIMAL_VALUE_STATIC * timeDifference)) {
      return Optional.of(euclidianDistance);
    } else {
      // except the ball
      if (actualObject.getType() == 7) {
        return Optional.of(euclidianDistance);
      } else {
        return Optional.empty();
      }
    }
  }

  /**
   * calculates the acceleration between to points
   * math: acceleration [m/s^2] = delta velocity [m/s]/ delta time [s] (linear acceleration)
   * @param actualObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param actualUtc UTC time as a String
   * @param oldObject of type Object
   * <Obj type="7" id="0" x="4111" y="2942" z="11" sampling="0" />
   * @param oldUtc UTC time as a String
   * @return Optional acceleration [m/s^2] or empty
   */
  private static Optional<Double> calcAcceleration(Object actualObject, String actualUtc,
      Object oldObject, String oldUtc) {
    double timeDifference = getTimeDifference(actualUtc, oldUtc);

    if (oldObject.getVelocity() == null || actualObject.getVelocity() == null || timeDifference == 0 ) {
      return Optional.empty();
    } else {
      double velocityDifference = actualObject.getVelocity() - oldObject.getVelocity();
      return Optional.of(velocityDifference / timeDifference);
    }
  }

}




