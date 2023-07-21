package com.mas2022datascience.tracabgen5enricher.processor;

import com.mas2022datascience.avro.v1.GeneralMatchPhase;
import com.mas2022datascience.avro.v1.Object;
import com.mas2022datascience.avro.v1.TracabGen5TF01;
import com.mas2022datascience.util.Distance;
import com.mas2022datascience.util.Vector;
import com.mas2022datascience.util.Time;
import com.mas2022datascience.util.Zones;
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
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
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
  @Value(value = "${topic.general-match-phase.name}") private String topicGeneralMatchPhase;

  @Bean
  public KStream<String, TracabGen5TF01> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);
    final Serde<TracabGen5TF01> tracabGen5TF01Serde = new SpecificAvroSerde<>();
    tracabGen5TF01Serde.configure(serdeConfig, false); // `false` for record values

    // match phase
    final Serde<GeneralMatchPhase> generalMatchPhaseSerde = new SpecificAvroSerde<>();
    generalMatchPhaseSerde.configure(serdeConfig, false); // `false` for record values
    KStream<String, GeneralMatchPhase> streamPhase = kStreamBuilder.stream(topicGeneralMatchPhase,
        Consumed.with(Serdes.String(), generalMatchPhaseSerde));
    KTable<String, GeneralMatchPhase> phases = streamPhase.toTable(Materialized.as("phasesStore"));

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
        .transform(() -> myStateHandler, myStateStore.name())
        .leftJoin(phases, (newValue, phaseValue) -> {
          for (Object object : newValue.getObjects()) {
            if (object.getTeamId() != null) {
              if (Zones.getZone(object.getX(), object.getY(),
                  newValue.getTs(), object.getTeamId(), phaseValue) != -1) {
                object.setZone(Zones.getZone(object.getX(), object.getY(),
                    newValue.getTs(), object.getTeamId(), phaseValue));
              }
            }
          }
          return newValue;
        });

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
          oldObjectsMap.put(object.getPlayerId(), object)
      );

      List<Object> actualObjects = value.getObjects();
      for (Object actualObject : actualObjects) {
        if (oldObjectsMap.containsKey(actualObject.getPlayerId())
            && actualObject.getType() != 3) { // Type 3 is a referee

          // update acceleration and distance in actual object
          actualObject.setAccelleration(calcAcceleration(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getPlayerId()),
              oldFrame.getUtc()).orElse(null)
          );

          // update distance to ball in actual object
          actualObject.setDistance(Distance.getEuclidianDistance(actualObject,
              value.getUtc(),
              oldObjectsMap.get(actualObject.getPlayerId()),
              oldFrame.getUtc()).orElse(null)
          );

          // update distance to ball in actual object
          Optional<Object> actualBallObject = actualObjects.stream()
              .filter(object -> object.getType() == 7).findFirst(); // Type 7 is the ball
          if (actualBallObject.stream().findFirst().isPresent()) {
            actualObject.setDistancePlayerBall(
                Distance.getEuclidianDistanceNoChecks(actualObject,
                    actualBallObject.stream().findFirst().get())
            );
          }

          // update player vector in actual object
          double[] playerVector = Vector.vector3dOf2Points(
              new double[]{oldObjectsMap.get(actualObject.getPlayerId()).getX(),
                  oldObjectsMap.get(actualObject.getPlayerId()).getY(),
                  oldObjectsMap.get(actualObject.getPlayerId()).getZ()},
              new double[]{actualObject.getX(), actualObject.getY(),
                  actualObject.getZ()});
          actualObject.setPlayerVectorX(playerVector[0]);
          actualObject.setPlayerVectorY(playerVector[1]);
          actualObject.setPlayerVectorZ(playerVector[2]);

          // update projection of player vector in direction of ball vector in actual object
          if (actualBallObject.stream().findFirst().isPresent() && oldObjectsMap.get("0") != null) {
            double[] ballVector = Vector.vector3dOf2Points(
                new double[]{actualBallObject.stream().findFirst().get().getX(),
                    actualBallObject.stream().findFirst().get().getY(),
                    actualBallObject.stream().findFirst().get().getZ()
                },
                new double[]{oldObjectsMap.get("0").getX(),
                    oldObjectsMap.get("0").getY(),
                    oldObjectsMap.get("0").getZ()
                });
            double[] projection = Vector.projectionOf3dVectorsOnAnother3dVector(playerVector, ballVector);
            actualObject.setPlayerBallVectorX(projection[0]);
            actualObject.setPlayerBallVectorY(projection[1]);
            actualObject.setPlayerBallVectorZ(projection[2]);
          }
        }
      }

      stateStore.put(key, value);
      return new KeyValue<>(key, stateStore.get(key));

    }

    @Override
    public void close() { }
  }

  /**
   * calculates the acceleration between two points
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
    double timeDifference = Time.getTimeDifference(actualUtc, oldUtc);

    if (oldObject.getVelocity() == null || actualObject.getVelocity() == null || timeDifference == 0 ) {
      return Optional.empty();
    } else {
      double velocityDifference = actualObject.getVelocity() - oldObject.getVelocity();
      return Optional.of(velocityDifference / timeDifference);
    }
  }

}




