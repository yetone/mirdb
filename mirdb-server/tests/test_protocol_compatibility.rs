#[cfg(test)]
mod protocol_compatibility_tests {
    use mirdb_server::parser::parse;
    use mirdb_server::proto::ServerCodec;
    use mirdb_server::request::{GetterType, Request, SetterType};
    use mirdb_server::response::{BufferWriter, GetRespItem, Response};
    use mirdb_server::slice::Slice;
    use mirdb_server::store::Store;
    use mirdb_server::test_utils::get_test_opt;

    use bytes::BytesMut;
    use tokio_io::codec::{Decoder, Encoder};

    #[test]
    fn test_set_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Simulate a SET command: "set key1 0 0 5\r\nvalue1\r\n"
        let set_command = b"set key1 0 0 5\r\nvalue1\r\n";
        let mut buf = BytesMut::from(set_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::Stored);
    }

    #[test]
    fn test_get_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // First set a value
        let response = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("key1"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("value1"),
            no_reply: false,
        });
        assert_eq!(response, Ok(Response::Stored));

        // Simulate a GET command: "get key1\r\n"
        let get_command = b"get key1\r\n";
        let mut buf = BytesMut::from(get_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();

        match response {
            Response::Get(items) => {
                assert_eq!(items.len(), 1);
                let item = &items[0];
                assert_eq!(item.key.as_ref(), b"key1");
                assert_eq!(item.data.as_ref(), b"value1");
                assert_eq!(item.flags, 0);
                assert_eq!(item.bytes, 5);
            }
            _ => panic!("Expected Get response, got {:?}", response),
        }
    }

    #[test]
    fn test_multi_key_get_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Set multiple values
        let _ = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("key1"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("value1"),
            no_reply: false,
        }).unwrap();

        let _ = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("key2"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("value2"),
            no_reply: false,
        }).unwrap();

        let _ = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("key3"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("value3"),
            no_reply: false,
        }).unwrap();

        // Simulate a multi-key GET command: "get key1 key2 key3\r\n"
        let get_command = b"get key1 key2 key3\r\n";
        let mut buf = BytesMut::from(get_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();

        match response {
            Response::Get(items) => {
                assert_eq!(items.len(), 3);
                assert_eq!(items[0].key.as_ref(), b"key1");
                assert_eq!(items[1].key.as_ref(), b"key2");
                assert_eq!(items[2].key.as_ref(), b"key3");
                assert_eq!(items[0].data.as_ref(), b"value1");
                assert_eq!(items[1].data.as_ref(), b"value2");
                assert_eq!(items[2].data.as_ref(), b"value3");
            }
            _ => panic!("Expected Get response with 3 items, got {:?}", response),
        }
    }

    #[test]
    fn test_add_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Test ADD with non-existent key (should succeed)
        let add_command = b"add newkey 0 0 5\r\nvalue1\r\n";
        let mut buf = BytesMut::from(add_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::Stored);

        // Test ADD with existing key (should fail)
        let add_command2 = b"add newkey 0 0 5\r\nvalue2\r\n";
        let mut buf2 = BytesMut::from(add_command2);
        let req2 = codec.decode(&mut buf2).unwrap().unwrap();

        let response2 = store.apply(req2).unwrap();
        assert_eq!(response2, Response::NotStored);
    }

    #[test]
    fn test_replace_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Test REPLACE with non-existent key (should fail)
        let replace_command = b"replace nonexistent 0 0 5\r\nvalue1\r\n";
        let mut buf = BytesMut::from(replace_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::NotStored);

        // Set a key first
        let _ = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("replace_key"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("first"),
            no_reply: false,
        }).unwrap();

        // Test REPLACE with existing key (should succeed)
        let replace_command2 = b"replace replace_key 0 0 6\r\nsecond\r\n";
        let mut buf2 = BytesMut::from(replace_command2);
        let req2 = codec.decode(&mut buf2).unwrap().unwrap();

        let response2 = store.apply(req2).unwrap();
        assert_eq!(response2, Response::Stored);
    }

    #[test]
    fn test_delete_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Set a key first
        store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("delete_key"),
            flags: 0,
            ttl: 0,
            bytes: 5,
            payload: Slice::from("value"),
            no_reply: false,
        }).unwrap();

        // Test DELETE command: "delete delete_key\r\n"
        let delete_command = b"delete delete_key\r\n";
        let mut buf = BytesMut::from(delete_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::Deleted);

        // Verify key is deleted
        let get_response = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from("delete_key")],
        });

        match get_response {
            Ok(Response::Get(items)) => assert_eq!(items.len(), 0),
            _ => panic!("Expected empty GET response after delete"),
        }
    }

    #[test]
    fn test_append_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Set initial value
        store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("append_key"),
            flags: 0,
            ttl: 0,
            bytes: 6,
            payload: Slice::from("prefix"),
            no_reply: false,
        }).unwrap();

        // Append to it
        let append_command = b"append append_key 0 0 6\r\n_suffix\r\n";
        let mut buf = BytesMut::from(append_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::Stored);

        // Verify the result
        let get_response = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from("append_key")],
        }).unwrap();

        match get_response {
            Response::Get(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].data.as_ref(), b"prefix_suffix");
            }
            _ => panic!("Expected Get response"),
        }
    }

    #[test]
    fn test_prepend_command_through_protocol() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Set initial value
        store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("prepend_key"),
            flags: 0,
            ttl: 0,
            bytes: 6,
            payload: Slice::from("_suffix"),
            no_reply: false,
        }).unwrap();

        // Prepend to it
        let prepend_command = b"prepend prepend_key 0 0 6\r\nprefix\r\n";
        let mut buf = BytesMut::from(prepend_command);

        let mut codec = ServerCodec;
        let req = codec.decode(&mut buf).unwrap().unwrap();

        let response = store.apply(req).unwrap();
        assert_eq!(response, Response::Stored);

        // Verify the result
        let get_response = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from("prepend_key")],
        }).unwrap();

        match get_response {
            Response::Get(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].data.as_ref(), b"prefix_suffix");
            }
            _ => panic!("Expected Get response"),
        }
    }

    #[test]
    fn test_ttl_expiration() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Set with 1 second TTL
        store.apply(Request::Setter {
            setter: SetterType::Set,
            key: Slice::from("ttl_key"),
            flags: 0,
            ttl: 1,
            bytes: 5,
            payload: Slice::from("value"),
            no_reply: false,
        }).unwrap();

        // Should be gettable immediately
        let get_response1 = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![Slice::from("ttl_key")],
        }).unwrap();

        match get_response1 {
            Response::Get(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].data.as_ref(), b"value");
            }
            _ => panic!("Expected Get response"),
        }
    }

    #[test]
    fn test_info_command() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        let info_request = Request::Info;
        let response = store.apply(info_request).unwrap();

        match response {
            Response::Info(info_str) => {
                assert!(!info_str.is_empty(), "INFO should return data");
                assert!(info_str.contains("Total level count") || info_str.contains("L0"), "INFO should contain level info");
            }
            _ => panic!("Expected Info response, got {:?}", response),
        }
    }

    #[test]
    fn test_protocol_encoding_decoding() {
        // Test that responses can be encoded properly
        let mut buf = BytesMut::new();
        let mut writer = BufferWriter::new(&mut buf);

        let response = Response::Stored;
        response.write(&mut writer).unwrap();

        assert_eq!(buf, b"STORED\r\n");

        // Test GET response encoding
        let mut buf2 = BytesMut::new();
        let mut writer2 = BufferWriter::new(&mut buf2);

        let get_response = Response::Get(vec![GetRespItem::new(
            Slice::from("key1"),
            Slice::from("value1"),
            0,
            6,
        )]);

        get_response.write(&mut writer2).unwrap();

        let expected = b"VALUE key1 0 6\r\nvalue1\r\nEND\r\n";
        assert_eq!(buf2, expected);
    }

    #[test]
    fn test_all_commands_through_store() {
        let opt = get_test_opt();
        let store = Store::new(opt).unwrap();

        // Test complete cycle: SET -> GET -> DELETE
        let key = Slice::from("integration_key");
        let value = Slice::from("integration_value");

        // SET
        let response = store.apply(Request::Setter {
            setter: SetterType::Set,
            key: key.clone(),
            flags: 0,
            ttl: 0,
            bytes: value.len(),
            payload: value.clone(),
            no_reply: false,
        });
        assert_eq!(response, Ok(Response::Stored));

        // GET
        let response = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![key.clone()],
        });
        match response {
            Ok(Response::Get(items)) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].data, value);
            }
            _ => panic!("Expected Get response"),
        }

        // DELETE
        let response = store.apply(Request::Deleter {
            key: key.clone(),
            no_reply: false,
        });
        assert_eq!(response, Ok(Response::Deleted));

        // GET after delete (should be empty)
        let response = store.apply(Request::Getter {
            getter: GetterType::Get,
            keys: vec![key.clone()],
        });
        match response {
            Ok(Response::Get(items)) => {
                assert_eq!(items.len(), 0);
            }
            _ => panic!("Expected empty Get response"),
        }
    }
}
