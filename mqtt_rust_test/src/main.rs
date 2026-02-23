use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
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

    // ✅ BROKER REAL
    let mut mqttoptions =
        MqttOptions::new(format!("client-{}", id), "192.168.100.10", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(60));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    println!("Client {} connecting to broker...", id);

    // =====================================================
    // INTENTO ÚNICO DE CONEXIÓN
    // =====================================================

    let mut connected = false;

    // solo intentamos unos cuantos polls
    for _ in 0..5 {
        match eventloop.poll().await {

            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                connected = true;
                println!("Client {} connected!", id);
                break;
            }

            Ok(_) => {}

            Err(e) => {
                println!("Client {} connection error: {}", id, e);
                break;
            }
        }

        sleep(Duration::from_millis(500)).await;
    }

    // ❌ si no conecta → terminar
    if !connected {
        println!("Client {} could NOT connect to broker.", id);
        std::process::exit(1);
    }

    // =====================================================
    // SUBSCRIBER
    // =====================================================
    if mode == "sub" {

        client.subscribe("test", QoS::AtLeastOnce)
            .await
            .unwrap();

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(_))) => {}
                Ok(_) => {}
                Err(e) => {
                    println!("Subscriber {} disconnected: {}", id, e);
                    break;
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

        // 🔥 EVENTLOOP EN BACKGROUND
        tokio::spawn(async move {
            loop {
                if let Err(e) = eventloop.poll().await {
                    println!("Publisher eventloop error: {}", e);
                    break;
                }
            }
        });

        let start = Instant::now();
        let mut message_count = 0u64;

        while start.elapsed().as_secs() < exec_time {

            if let Err(e) = client.publish(
                "test",
                QoS::AtLeastOnce,
                false,
                payload.clone(),
            ).await {
                println!("Publish failed: {}", e);
                break;
            }

            message_count += 1;

            sleep(delay).await;
        }

        println!("Total messages sent: {}", message_count);

        client.disconnect().await.ok();

        std::process::exit(0);
    }
}