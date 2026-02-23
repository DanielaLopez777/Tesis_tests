use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() {

    // ======================
    // CONFIG
    // ======================

    let broker_ip = "192.168.100.10";
    let exec_time = 20;
    let payload_size = 100;
    let freq = 0.5;

    let payload = vec![b'a'; payload_size];
    let delay = Duration::from_secs_f64(freq);

    // ======================
    // MQTT OPTIONS
    // ======================

    let mut mqttoptions =
        MqttOptions::new("rumqtt-simple-client", broker_ip, 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    println!("Connecting to {}...", broker_ip);

    // ======================
    // ESPERAR CONNACK REAL
    // ======================

    loop {
        match eventloop.poll().await {

            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                println!("✅ CONNECTED TO RUMQTT BROKER");
                break;
            }

            Ok(_) => {}

            Err(e) => {
                println!("Broker not available: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // ======================
    // CONTADORES
    // ======================

    let start = Instant::now();
    let mut messages_sent = 0u64;

    println!("Publishing messages...");

    // ======================
    // LOOP MQTT REAL
    // ======================

    while start.elapsed().as_secs() < exec_time {

        // mantener conexión MQTT viva
        match eventloop.poll().await {
            Ok(_) => {}
            Err(e) => {
                println!("Connection lost: {}", e);
                break;
            }
        }

        // publicar
        if let Err(e) = client.publish(
            "test",
            QoS::AtLeastOnce,
            false,
            payload.clone(),
        ).await {
            println!("Publish error: {}", e);
            break;
        }

        messages_sent += 1;

        sleep(delay).await;
    }

    // ======================
    // RESULTADOS
    // ======================

    let elapsed = start.elapsed().as_secs_f64();

    println!("\n===== RESULTS =====");
    println!("Time: {:.2}s", elapsed);
    println!("Messages sent: {}", messages_sent);
    println!("Throughput: {:.2} msg/s", messages_sent as f64 / elapsed);

    client.disconnect().await.ok();
}