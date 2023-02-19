use std::time::Duration;

use message_buffer::{service_fn, ConstantBackOff, MessageBuffer, Messages, Options};
use tokio::time;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut mb = MessageBuffer::new(
        service_fn(process),
        ConstantBackOff::new(Duration::from_secs(1)),
        Options::default(),
    );
    for i in 0..10 {
        println!("=========");
        if let Err(e) = mb.push(i).await {
            println!("push err: {e}")
        }
        println!("pushed");
        time::sleep(Duration::from_secs(1)).await;
    }
    mb.stop().await;
    Ok(())
}

async fn process(mut m: Messages<usize>) -> Result<(), String> {
    let msgs = m.msgs();
    for msg in msgs {
        println!("received {}", msg.data);
        if msg.trys > 1 {
            continue;
        }
        if let Err(e) = m.retry(&msg).await {
            println!("process retry err: {e}")
        }
        println!("send retry");
    }
    Ok(())
}
