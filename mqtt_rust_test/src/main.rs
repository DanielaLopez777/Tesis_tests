use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use tokio::time::{sleep, timeout, Duration, Instant};
use std::process;

#[tokio::main]
async fn main() {

    println!("Iniciando cliente MQTT...");

    // ===============================
    // CONFIGURACIÓN DEL BROKER
    // ===============================
    let mut mqttoptions =
        MqttOptions::new("cliente_rumqtt", "192.168.100.10", 1883);

    mqttoptions.set_keep_alive(Duration::from_secs(5));

    // Buffer interno
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // ===============================
    // INTENTO DE CONEXIÓN CON TIMEOUT
    // ===============================
    println!("Intentando conectar...");

    let conexion = timeout(Duration::from_secs(3), eventloop.poll()).await;

    match conexion {
        Ok(Ok(Event::Incoming(Packet::ConnAck(_)))) => {
            println!("✅ Conectado al broker");
        }
        Ok(Ok(_)) => {
            println!("⚠ Evento inesperado");
        }
        Ok(Err(e)) => {
            eprintln!("❌ Error de conexión: {:?}", e);
            process::exit(1);
        }
        Err(_) => {
            eprintln!("❌ Timeout: No se pudo conectar al broker");
            process::exit(1);
        }
    }

    // ===============================
    // CONTADORES
    // ===============================
    let inicio = Instant::now();
    let mut mensajes_enviados = 0u64;

    // ===============================
    // LOOP PRINCIPAL
    // ===============================
    for i in 1..=10 {

        let payload = format!("Mensaje {}", i);

        match client.publish(
            "test/topic",
            QoS::AtLeastOnce,
            false,
            payload
        ).await {
            Ok(_) => {
                mensajes_enviados += 1;
                println!("📤 Mensaje enviado {}", mensajes_enviados);
            }
            Err(e) => {
                eprintln!("❌ Error publicando: {:?}", e);
                break;
            }
        }

        // Procesar eventos MQTT
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::PubAck(_))) => {
                println!("✔ ACK recibido");
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("❌ Conexión perdida: {:?}", e);
                break;
            }
        }

        sleep(Duration::from_secs(1)).await;
    }

    // ===============================
    // ESTADÍSTICAS
    // ===============================
    let tiempo_total = inicio.elapsed().as_secs_f64();

    println!("\n========== RESUMEN ==========");
    println!("Mensajes enviados : {}", mensajes_enviados);
    println!("Tiempo total      : {:.2} s", tiempo_total);
    println!(
        "Mensajes/segundo  : {:.2}",
        mensajes_enviados as f64 / tiempo_total
    );

    // ===============================
    // CIERRE LIMPIO MQTT
    // ===============================
    println!("Cerrando conexión MQTT...");
    client.disconnect().await.unwrap();

    println!("✅ Cliente finalizado correctamente");
}