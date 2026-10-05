use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, BufReader};
use bytes::Buf;
use server::wire::{Payload, decode_op_query_payload, decode_op_msg_payload, decode_op_reply_payload};

#[tokio::main]
async fn main() {
  let listener = TcpListener::bind("127.0.0.1:27017").await.unwrap();

  loop {
    let (socket, _) = listener.accept().await.unwrap();

    tokio::spawn(async move {
        process(socket).await;
    });
  }
}

async fn process(stream: TcpStream) {
  let mut reader = BufReader::new(stream);
  
  loop {
    let mut header = [0u8; 16];
    if reader.read_exact(&mut header).await.is_err() {
      break;
    }

    println!("{:?}", header);

    let mut cursor = &header[..];
    let message_length = cursor.get_u32_le() as usize;
    let _request_id = cursor.get_u32_le();
    let _response_to = cursor.get_u32_le();
    let op_code = cursor.get_u32_le();

    if message_length < 16 {
      break;
    }

    let mut payload = vec![0u8; message_length - 16];
    if reader.read_exact(&mut payload).await.is_err() {
      break;
    }

    println!("{:?}", payload);

    let payload: Result<Payload, Box<dyn std::error::Error>> = match op_code {
      2004 => decode_op_query_payload(&payload),
      1 => decode_op_reply_payload(&payload),
      2013 => decode_op_msg_payload(&payload),
      _ => Err("unknown opcode".into()),
    };

    println!("{:?}", payload);

  }
}

