
use bson::Document;
use bytes::Buf;

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

pub fn decode_op_query_payload(payload: &Vec<u8>) -> Result<Payload, Box<dyn std::error::Error>> {
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

pub fn decode_op_reply_payload(payload: &[u8]) -> Result<Payload, Box<dyn std::error::Error>> {
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

pub fn decode_op_msg_payload(payload: &[u8]) -> Result<Payload, Box<dyn std::error::Error>> {
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
) -> Result<Vec<OpMsgSection>, Box<dyn std::error::Error>> {
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

fn read_null_terminated_string(buf: &mut &[u8]) -> Result<String, Box<dyn std::error::Error>> {
  let pos = buf
    .iter()
    .position(|&b| b == 0)
    .ok_or("Unterminated null-terminated string")?;

  let s = std::str::from_utf8(&buf[..pos])?.to_string();
  *buf = &buf[pos + 1..];
  Ok(s)
}

