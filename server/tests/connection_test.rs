use mongodb::{Client, bson::doc};

#[tokio::test]
async fn test_server_connection() {
  let client_uri = "mongodb://localhost:27017";
  let client = Client::with_uri_str(client_uri).await.expect("Failed to initialize client");

  let db = client.database("admin");
  let result = db.run_command(doc! { "ping": 1 }).await;

  assert!(result.is_ok(), "Connection and ping failed: {:?}", result.err());
}