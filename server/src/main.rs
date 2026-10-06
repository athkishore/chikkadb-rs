use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use bytes::Buf;
use server::wire::{
  Payload,
  decode_op_query_payload,
  decode_op_msg_payload,
  decode_op_reply_payload,
  get_encoded_response,
};

#[tokio::main]
async fn main() {
  let listener = TcpListener::bind("127.0.0.1:27017").await.unwrap();

  loop {
    let (socket, _) = listener.accept().await.unwrap();

    tokio::spawn(async move {
      let _ = process(socket).await;
    });
  }
}

async fn process(stream: TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
  let (reader, mut writer) = tokio::io::split(stream);
  let mut reader = BufReader::new(reader);
  
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

    let payload: Result<Payload, Box<dyn std::error::Error + Send + Sync>> = match op_code {
      2004 => decode_op_query_payload(&payload),
      1 => decode_op_reply_payload(&payload),
      2013 => decode_op_msg_payload(&payload),
      _ => Err("unknown opcode".into()),
    };

    println!("{:?}", payload);

    if let Ok(payload) = payload {
      if let Ok(response_buf) = get_encoded_response(payload).await {
        println!("{:?}", response_buf);
        if let Err(err) = writer.write_all(&response_buf).await {
          eprint!("{:?}", err);
        };
      }
    }

  }

  Ok(())
}

