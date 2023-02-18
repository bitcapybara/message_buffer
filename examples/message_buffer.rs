use std::time::Duration;

use message_buffer::{service_fn, ConstantBackOff, Item, MessageBuffer, Messages, Options};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut mb = MessageBuffer::new(
        service_fn(process),
        ConstantBackOff::new(Duration::from_secs(1)),
        Options::default(),
    );
    mb.push(2).await?;

    Ok(())
}

async fn process(mut m: Messages<usize>) -> Result<Vec<Item<usize>>, String> {
    let msgs = m.msgs();
    for msg in msgs {
        m.retry(&msg);
    }
    Ok(m.retries())
}
