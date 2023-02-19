use std::time::Duration;

use message_buffer::{service_fn, ConstantBackOff, Item, MessageBuffer, Messages, Options};
use tokio::time;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut mb = MessageBuffer::new(
        service_fn(process),
        ConstantBackOff::new(Duration::from_secs(1)),
        Options::default(),
    );
    mb.push(2).await?;
    time::sleep(Duration::from_secs(4)).await;
    mb.stop().await;
    Ok(())
}

async fn process(mut m: Messages<usize>) -> Result<Vec<Item<usize>>, String> {
    let msgs = m.msgs();
    for msg in msgs {
        println!("received {}", msg.data)
    }
    Ok(m.retries())
}
