use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncBufReadExt, BufReader};

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
    let mut buf_reader = BufReader::new(stream);
    let command = buf_reader.fill_buf().await;
    println!("{:?}", command);
}