use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};
use std::{env, time::{Duration, Instant}};
use tokio::time::sleep;

#[tokio::main]
async fn main() {

    let args: Vec<String> = env::args().collect();

    let mode = &args[1];
    let id = &args[2];

    let mut mqttoptions =
        MqttOptions::new(format!("client-{}", id), "localhost", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(60));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // =====================================================
    // SUBSCRIBER
    // =====================================================
    if mode == "sub" {

        client.subscribe("test", QoS::AtLeastOnce).await.unwrap();

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(_))) => {}
                Ok(_) => {}
                Err(_) => break,
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

        // EVENTLOOP EN THREAD SEPARADO (CLAVE)
        let mut ev = eventloop;
        tokio::spawn(async move {
            loop {
                let _ = ev.poll().await;
            }
        });

        // PUBLICACIÓN CONTROLADA
        while start.elapsed().as_secs() < exec_time {

            client
                .publish("test", QoS::AtLeastOnce, false, payload.clone())
                .await
                .unwrap();

            sleep(delay).await;
        }

        println!("Publisher finished");
        client.disconnect().await.unwrap();

        std::process::exit(0);
    }
}