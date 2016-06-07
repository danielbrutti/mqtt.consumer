package com.mateam.mqtt.consumer;

import com.google.common.eventbus.Subscribe;
import com.mateam.mqtt.connector.config.MqttConfiguration;
import com.mateam.mqtt.connector.connection.MqttConnector;
import com.mateam.mqtt.connector.events.MqttPacketAvailableEvent;
import com.mateam.mqtt.connector.exceptions.ConnectionException;
import com.mateam.mqtt.connector.util.LinuxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.util.UUID;

@SpringBootApplication
@EnableScheduling
@EnableAsync
@ContextConfiguration(classes = {MqttConfiguration.class}, loader = AnnotationConfigContextLoader.class)
@ComponentScan(basePackages = {"com.mateam.mqtt"})
@EnableAutoConfiguration
public class Application {

    Logger logger = LoggerFactory.getLogger(Application.class);

    @Autowired
    private MqttConnector connector;

    @Value("${mqtt.consumer.topic:messages/}")
    private String topic;

    @Autowired
    private MqttMessageProcessor mqttMessageProcessor;

    @Value("${mqtt.broker.url:tcp://localhost:1883}")
    private String mqttBrokerURL;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Application.class);
    }

    @Bean
    public CommandLineRunner run() {
        return new CommandLineRunner() {
            @Override
            public void run(String... args) throws Exception {

                if (connector.isConnected() == false) {
                    logger.info("Connecting to MQTT");
                    openConnection();
                }

            }
        };
    }

    private void openConnection() {
        logger.info("Open MQTT Connection");

        String macAddress;
        try {
            macAddress = LinuxUtils.getHostMacAddress() + "-CONSUMER";
        } catch (Exception e) {
            macAddress = UUID.randomUUID().toString();
        }

        connector.initiliaze(mqttBrokerURL, null, null, "lastwill/" + macAddress, macAddress, false, "/tmp");

        try {
            connector.connect();

            connector.subscribe("lastwill/#", 1);
            connector.subscribe(topic, 1);

            connector.registerListener(this);

            logger.info("Connection opened");
        } catch (ConnectionException e1) {
            logger.error("Error when opening MQTT connection", e1);
        }
    }

    @Subscribe
    public void onPacketArrive(MqttPacketAvailableEvent event) {
        mqttMessageProcessor.process(event.getMqttPacket());
    }

}
