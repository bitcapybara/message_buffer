use std::time::Duration;

use message_buffer::{processor_fn, ConstantBackOff, MessageBuffer, Messages, Options};
use tokio::time;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mb = MessageBuffer::new(
        processor_fn(process),
        ConstantBackOff::new(Duration::from_secs(1)),
        Options::default(),
    );
    for i in 0..10 {
        println!("=========");
        if let Err(e) = mb.try_push(i).await {
            println!("push err: {e}")
        }
        println!("pushed");
        time::sleep(Duration::from_secs(1)).await;
    }
    mb.stop().await?;
    Ok(())
}

async fn process(mut m: Messages<usize>) -> Result<(), String> {
    for msg in m.msgs() {
        println!("received {}", msg.data);
        if let Err(e) = m.add_retry(&msg).await {
            println!("process retry err: {e}")
        }
        println!("send retry");
    }
    Ok(())
}
