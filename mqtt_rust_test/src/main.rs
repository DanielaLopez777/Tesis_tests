use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::{
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{task, time::sleep};

#[tokio::main]
async fn main() {

    // ======================
    // ARGUMENTOS (OWNED VALUES)
    // ======================

    let args: Vec<String> = env::args().collect();

    let mode = args[1].clone();
    let id = args[2].clone();

    // ======================
    // MQTT OPTIONS
    // ======================

    let mut mqttoptions =
        MqttOptions::new(format!("client-{}", id), "192.158.100.10", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    let connected = Arc::new(AtomicBool::new(false));
    let connected_clone = connected.clone();

    let id_clone = id.clone();

    // ======================
    // EVENT LOOP TASK
    // ======================

    task::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    println!("Client {} connected", id_clone);
                    connected_clone.store(true, Ordering::Relaxed);
                }
                Ok(_) => {}
                Err(e) => {
                    println!("Connection error {:?}", e);
                    connected_clone.store(false, Ordering::Relaxed);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    });

    // esperar conexión real
    while !connected.load(Ordering::Relaxed) {
        sleep(Duration::from_millis(100)).await;
    }

    // ======================
    // SUBSCRIBER
    // ======================

    if mode == "sub" {

        client.subscribe("test", QoS::AtLeastOnce).await.unwrap();

        println!("Subscriber {} running", id);

        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }

    // ======================
    // PUBLISHER
    // ======================

    if mode == "pub" {

        let payload_size: usize = args[3].parse().unwrap();
        let exec_time: u64 = args[4].parse().unwrap();
        let freq: f64 = args[5].parse().unwrap();

        let payload = vec![b'a'; payload_size];
        let delay = Duration::from_secs_f64(freq);

        let start = Instant::now();
        let mut sent = 0;

        println!("Publisher {} started", id);

        while start.elapsed().as_secs() < exec_time {

            match client
                .publish("test", QoS::AtLeastOnce, false, payload.clone())
                .await
            {
                Ok(_) => sent += 1,
                Err(e) => println!("Publish error {:?}", e),
            }

            sleep(delay).await;
        }

        println!("Publisher {} sent {}", id, sent);
    }
}