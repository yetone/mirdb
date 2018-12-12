use std::str;

pub fn to_str(cs: &[u8]) -> &str {
    str::from_utf8(&cs).expect("not a valid utf8")
}
