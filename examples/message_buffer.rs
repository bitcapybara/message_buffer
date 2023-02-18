use std::time::Duration;

use message_buffer::{ConstantBackOff, MessageBuffer, Messages, Options};

fn main() {
    // let mb = MessageBuffer::new(
    //     process,
    //     ConstantBackOff::new(Duration::from_secs(1)),
    //     Options::default(),
    // );
}

async fn process(msgs: &mut Messages<usize>) -> Result<(), String> {
    Ok(())
}
