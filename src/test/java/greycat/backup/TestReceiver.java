package greycat.backup;

import greycat.Graph;
import greycat.GraphBuilder;
import greycat.backup.tools.StorageHandler;
import greycat.struct.Buffer;
import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;

public class TestReceiver {

    public static void main(String[] args )
    {
        try {
            Connection nc = Nats.connect();

            Graph localGraph = GraphBuilder.newBuilder().build();

            StorageHandler storageHandler = new StorageHandler();
            storageHandler.load();

            nc.subscribe("Greycat", m -> {
                Buffer buffer = localGraph.newBuffer();
                buffer.writeAll(m.getData());

                try {
                    storageHandler.process(buffer);
                } catch (Exception e) {
                    try {
                        nc.publish("Greycat", m.getData());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    e.printStackTrace();
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                public void run() {
                    storageHandler.close();
                }
            }));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
