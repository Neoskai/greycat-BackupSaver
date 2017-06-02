/**
 * Copyright 2017 The GreyCat Authors.  All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package greycat.backup.consumer;

import greycat.Graph;
import greycat.GraphBuilder;
import greycat.backup.tools.StorageHandler;
import greycat.struct.Buffer;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class UTTReceiver {
    public static void main(String[] args )
    {
        try {
            MqttClient client = new MqttClient(
                    "tcp://broker.mqttdashboard.com:1883", //URI
                    MqttClient.generateClientId(), //ClientId
                    new MemoryPersistence()); //Persistence

            Graph localGraph = GraphBuilder.newBuilder().build();

            StorageHandler storageHandler = new StorageHandler();
            storageHandler.load();

            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    //Called when the client lost the connection to the broker

                }

                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    Buffer buffer = localGraph.newBuffer();
                    buffer.writeAll(message.getPayload());

                    storageHandler.process(buffer);
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    //Called when a outgoing publish is complete
                }
            });

            client.connect();
            client.subscribe("greycat", 2);

        } catch (MqttException e) {
            e.printStackTrace();
        }
    }
}
