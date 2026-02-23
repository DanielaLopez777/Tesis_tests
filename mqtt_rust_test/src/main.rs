use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::{
    env,
    time::{Duration, Instant},
};
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage:");
        println!("Subscriber -> sub <id>");
        println!("Publisher  -> pub <id> <payload> <exec_time> <freq>");
        return;
    }

    let mode = &args[1];
    let id = &args[2];

    // BROKER REAL
    let mut mqttoptions =
        MqttOptions::new(format!("client-{}", id), "192.168.100.10", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(10));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // =====================================================
    // ESPERAR CONEXIÓN REAL (CONNACK)
    // =====================================================
    println!("Connecting to broker...");

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                println!("Client {} connected!", id);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                println!("Connection error: {}", e);
                sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // =====================================================
    // SUBSCRIBER
    // =====================================================
    if mode == "sub" {
        client
            .subscribe("test", QoS::AtLeastOnce)
            .await
            .expect("Subscribe failed");

        println!("Subscriber {} listening...", id);

        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(_))) => {
                    // mensaje recibido
                }
                Ok(_) => {}
                Err(e) => {
                    println!("Subscriber {} error: {}", id, e);
                    break;
                }
            }
        }
    }

    // =====================================================
    // PUBLISHER
    // =====================================================
    if mode == "pub" {
        if args.len() < 6 {
            println!("Missing publisher arguments");
            return;
        }

        let payload_size: usize = args[3].parse().unwrap();
        let exec_time: u64 = args[4].parse().unwrap();
        let freq: f64 = args[5].parse().unwrap();

        let payload = vec![b'a'; payload_size];
        let delay = Duration::from_secs_f64(freq);

        // EVENTLOOP EN BACKGROUND
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(_) => {}
                    Err(e) => {
                        println!("Eventloop error: {}", e);
                        break;
                    }
                }
            }
        });

        println!("Publisher {} sending messages...", id);

        let start = Instant::now();
        let mut message_count = 0u64;

        while start.elapsed().as_secs() < exec_time {
            match client
                .publish("test", QoS::AtLeastOnce, false, payload.clone())
                .await
            {
                Ok(_) => {
                    message_count += 1;
                }
                Err(e) => {
                    println!("Publish error: {}", e);
                    break;
                }
            }

            sleep(delay).await;
        }

        println!("Total messages sent: {}", message_count);

        client.disconnect().await.ok();
    }
}