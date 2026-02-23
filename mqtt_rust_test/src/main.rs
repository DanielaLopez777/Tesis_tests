use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::{
    env,
    time::{Duration, Instant},
};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let mode = &args[1];
    let id = &args[2];

    let mut mqttoptions =
        MqttOptions::new(format!("client-{}", id), "192.168.100.10", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    println!("Client {} connecting...", id);

    let mut connected = false;

    // =====================================================
    // SUBSCRIBER
    // =====================================================
    if mode == "sub" {

        loop {
            match eventloop.poll().await {

                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("Subscriber {} connected", id);
                    connected = true;

                    client.subscribe("test", QoS::AtLeastOnce)
                        .await
                        .unwrap();
                }

                Ok(Event::Incoming(Packet::Publish(_))) => {}

                Ok(_) => {}

                Err(e) => {
                    println!("Subscriber {} error: {}", id, e);
                    connected = false;
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    // =====================================================
    // PUBLISHER
    // =====================================================
    if mode == "pub" {

        let payload_size: usize = args[3].parse().unwrap();
        let exec_time: u64 = args[4].parse().unwrap();
        let freq: f64 = args[5].parse().unwrap();

        let payload = vec![b'a'; payload_size];
        let delay = Duration::from_secs_f64(freq);

        let start = Instant::now();
        let mut message_count = 0u64;

        loop {

            // 🔥 MQTT REAL SUCEDE AQUÍ
            match eventloop.poll().await {

                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("Publisher {} connected", id);
                    connected = true;
                }

                Ok(_) => {}

                Err(e) => {
                    println!("Publisher {} disconnected: {}", id, e);
                    connected = false;
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }

            // ✅ SOLO PUBLICA SI EXISTE CONEXIÓN REAL
            if connected && start.elapsed().as_secs() < exec_time {

                if let Err(e) = client.publish(
                    "test",
                    QoS::AtLeastOnce,
                    false,
                    payload.clone(),
                ).await {
                    println!("Publish error: {}", e);
                    connected = false;
                } else {
                    message_count += 1;
                }

                sleep(delay).await;
            }

            if start.elapsed().as_secs() >= exec_time {
                break;
            }
        }

        println!("Total messages sent: {}", message_count);

        client.disconnect().await.ok();
    }
}