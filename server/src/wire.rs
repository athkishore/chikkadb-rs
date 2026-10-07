
use bson::{Document, doc, oid::ObjectId};
use bytes::Buf;
use std::io::{Write};
use std::sync::OnceLock;

fn get_process_id() -> &'static ObjectId {
  static PROCESS_ID: OnceLock<ObjectId> = OnceLock::new();
  PROCESS_ID.get_or_init(ObjectId::new)
}

#[derive(Debug)]
pub struct MessageHeader {
  pub message_length: usize,
  pub request_id: u32,
  pub response_to: u32,
  pub op_code: u32,
}

#[derive(Debug)]
pub enum Payload {
  OpQuery {
    flags: i32,
    full_collection_name: String,
    number_to_skip: i32,
    number_to_return: i32,
    query: Document,
    return_fields_selector: Option<Document>,
  },
  OpReply {
    response_flags: i32,
    cursor_id: i64,
    starting_from: i32,
    number_returned: i32,
    documents: Vec<Document>,
  },
  OpMsg {
    flag_bits: i32,
    sections: Vec<OpMsgSection>,
  },
}

#[derive(Debug)]
pub enum OpMsgSection {
  KindZero {
    document: Document,
  },
  KindOne {
    size: i32,
    document_sequence_identifier: String,
    documents: Vec<Document>,
  },
}

pub fn decode_op_query_payload(payload: &[u8]) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  let mut cursor = &payload[..];

  let flags = cursor.get_i32_le();
  let full_collection_name = read_null_terminated_string(&mut cursor)?;
  let number_to_skip = cursor.get_i32_le();
  let number_to_return = cursor.get_i32_le();

  let query = Document::from_reader(cursor)?;
  let return_fields_selector = if cursor.has_remaining() {
    Some(Document::from_reader(cursor)?)
  } else {
    None
  };

  Ok(Payload::OpQuery {
    flags,
    full_collection_name,
    number_to_skip,
    number_to_return,
    query,
    return_fields_selector,
  })
}

pub fn decode_payload(op_code: u32, payload_buf: &[u8]) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  match op_code {
    2004 => decode_op_query_payload(payload_buf),
    1 => decode_op_reply_payload(payload_buf),
    2013 => decode_op_msg_payload(payload_buf),
    _ => Err("unknown opcode".into()),
  }
}

pub fn decode_op_reply_payload(payload: &[u8]) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  let mut cursor = &payload[..];

  let response_flags = cursor.get_i32_le();
  let cursor_id = cursor.get_i64_le();
  let starting_from = cursor.get_i32_le();
  let number_returned = cursor.get_i32_le();
  
  let mut documents = Vec::with_capacity(number_returned.max(0) as usize);

  for _ in 0..number_returned {
    let doc = Document::from_reader(&mut cursor)?;
    documents.push(doc);
  }

  Ok(Payload::OpReply { 
    response_flags,
    cursor_id,
    starting_from,
    number_returned,
    documents
  })
}

pub fn decode_op_msg_payload(payload: &[u8]) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  let mut cursor = &payload[..];

  let flag_bits = cursor.get_i32_le();
  let sections = decode_op_msg_payload_sections(cursor)?;
  
  Ok(Payload::OpMsg { 
    flag_bits,
    sections 
  })
}

fn decode_op_msg_payload_sections(
  payload: &[u8]
) -> Result<Vec<OpMsgSection>, Box<dyn std::error::Error + Send + Sync>> {
  let mut sections = Vec::new();
  let mut cursor = &payload[..];

  while cursor.has_remaining() {
    let section_kind = cursor.get_u8();

    match section_kind {
      0 => {
        let document = Document::from_reader(&mut cursor)?;
        sections.push(OpMsgSection::KindZero { document })
      },
      1 => {
        let size = cursor.get_i32_le();
        let document_sequence_identifier = read_null_terminated_string(&mut cursor)?;

        let mut documents = Vec::new();

        let header_len = 4 + document_sequence_identifier.len() + 1;
        let target_len = size as usize;
        let mut current_consumed = header_len;

        while current_consumed < target_len && cursor.has_remaining() {
          let before_len = cursor.len();
          let doc = Document::from_reader(&mut cursor)?;
          let consumed = before_len - cursor.len();
          current_consumed += consumed;
          documents.push(doc);
        }

        sections.push(OpMsgSection::KindOne { 
          size,
          document_sequence_identifier,
          documents,
        })
      },
      _ => return Err("Unknown section kind".into()),
    }
  }

  Ok(sections)
}

fn read_null_terminated_string(buf: &mut &[u8]) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
  let pos = buf
    .iter()
    .position(|&b| b == 0)
    .ok_or("Unterminated null-terminated string")?;

  let s = std::str::from_utf8(&buf[..pos])?.to_string();
  *buf = &buf[pos + 1..];
  Ok(s)
}

pub async fn get_encoded_response(message_header: MessageHeader, payload: Payload) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
  let response_payload = get_response_payload(payload).await?;
  let response_header = MessageHeader {
    message_length: 0, // will be set by encoder
    request_id: 1, // TODO: Unhardcode - maintain state for each connection
    response_to: message_header.request_id,
    op_code: if message_header.op_code == 2004 { 1 } else { 2013 },
  };
  let encoded_response = encode_message(response_header, response_payload);
  Ok(encoded_response)
}

pub async fn get_response_payload(payload: Payload) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  match payload {
    Payload::OpQuery {..} => {
      Ok(Payload::OpReply { 
        response_flags: 0,
        cursor_id: 0,
        starting_from: 0,
        number_returned: 1,
        documents: Vec::from([
          doc! {
            "helloOk": true,
          },
        ]),
      })
    },
    Payload::OpMsg { sections, .. } => {
      let response = get_op_msg_response(sections).await;

      match response {
        Ok(response) => Ok(response),
        Err(err) => Err(err),
      }
    },
    _ => Err("Invalid opcode".into()),
  }
}

pub fn encode_message(header: MessageHeader, payload: Payload) -> Vec<u8> {
  println!("{:?}", header);
  println!("{:?}", payload);
  
  let payload_buf = match payload {
    Payload::OpMsg { flag_bits, sections } => {
      encode_op_msg_payload(Payload::OpMsg { flag_bits, sections })
    },
    Payload::OpQuery { .. } => {
      vec![]
    },
    Payload::OpReply { .. } => {
      vec![]
    },
  };

  let message_length = 16 + payload_buf.len();
  let mut response_message_buf = Vec::with_capacity(message_length);

  response_message_buf.extend_from_slice(&(message_length as u32).to_le_bytes());
  response_message_buf.extend_from_slice(&header.request_id.to_le_bytes());
  response_message_buf.extend_from_slice(&header.response_to.to_le_bytes());
  response_message_buf.extend_from_slice(&header.op_code.to_le_bytes());
  response_message_buf.extend_from_slice(&payload_buf);

  response_message_buf
}

pub fn encode_op_msg_payload(payload: Payload) -> Vec<u8> {
  if let Payload::OpMsg { flag_bits, sections } = payload {
    let mut buf = Vec::new();

    buf.extend_from_slice(&flag_bits.to_le_bytes());

    let sections_bytes = encode_op_msg_payload_sections(&sections);
    let _ = buf.write_all(&sections_bytes);

    buf
  } else {
    panic!("Expected an op_msg_payload");
  }
}

fn encode_op_msg_payload_sections(sections: &[OpMsgSection]) -> Vec<u8> {
  let mut sections_bytes = Vec::new();

  for section in sections {
    match section {
      OpMsgSection::KindZero { document } => {
        sections_bytes.push(0);

        let mut doc_bytes = Vec::new();
        let _ = document.to_writer(&mut doc_bytes);
        let _ = sections_bytes.write_all(&doc_bytes);
      },
      OpMsgSection::KindOne { 
        document_sequence_identifier,
        documents,
        ..
      } => {
        sections_bytes.push(1);

        let identifier_bytes = document_sequence_identifier.as_bytes();

        let mut documents_bytes = Vec::new();
        for doc in documents {
          let _ = doc.to_writer(&mut documents_bytes);
        }

        let size = 4 + identifier_bytes.len() + documents_bytes.len();
        let _ = sections_bytes.extend_from_slice(&(size as i32).to_le_bytes());
        let _ = sections_bytes.write_all(identifier_bytes);
        let _ = sections_bytes.write_all(&documents_bytes);
      }
    }
  }

  sections_bytes
}


pub async fn get_op_msg_response(_section: Vec<OpMsgSection>) -> Result<Payload, Box<dyn std::error::Error + Send + Sync>> {
  Ok(Payload::OpMsg {
    flag_bits: 0,
    sections: Vec::from([
      OpMsgSection::KindZero { 
        document: doc! {
          "helloOk": true,
          "isMaster": true,
          "topologyVersion": {
            "processId": get_process_id(),
            "counter": 0i64,
          },
          "maxBsonObjectSize": 16777216i32,
          "maxMessageSizeBytes": 48000000i32,
          "maxWriteBatchSize": 100000i32,
          "localTime": bson::DateTime::now(),
          "logicalSessionTimeoutMinutes": 30i32,
          "connectionId": 15i32,
          "minWireVersion": 0i32,
          "maxWireVersion": 21i32,
          "readOnly": false,
          "ok": 1i32,
        }
      },
    ]) 
  })
}