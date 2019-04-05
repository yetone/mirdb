use std::str;

pub fn to_str(cs: &[u8]) -> &str {
    str::from_utf8(&cs).expect("not a valid utf8")
}

pub fn make_file_name(num: usize, ext: &str) -> String {
    format!("{:08}.{}", num, ext)
}
