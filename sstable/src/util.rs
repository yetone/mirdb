use std::str;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;

pub fn to_str(cs: &[u8]) -> &str {
    str::from_utf8(&cs).expect("not a valid utf8")
}

pub fn find_shortest_sep(a: &[u8], b: &[u8]) -> Vec<u8> {
    if a == b {
        return a.to_vec();
    }

    let min = if a.len() < b.len() { a.len() } else { b.len() };
    let mut diff_at = 0;

    while diff_at < min && a[diff_at] == b[diff_at] {
        diff_at += 1;
    }

    while diff_at < min {
        let diff = a[diff_at];
        if diff < 0xff && diff + 1 < b[diff_at] {
            let mut sep = Vec::from(&a[0..diff_at + 1]);
            sep[diff_at] += 1;
            return sep;
        }

        diff_at += 1;
    }
    // Backup case: either `a` is full of 0xff, or all different places are less than 2
    // characters apart.
    // The result is not necessarily short, but a good separator: e.g., "abc" vs "abd" ->
    // "abc\0", which is greater than "abc" and lesser than "abd".
    let mut sep = Vec::with_capacity(a.len() + 1);
    sep.extend_from_slice(a);
    // Append a 0 byte; by making it longer than a, it will compare greater to it.
    sep.extend_from_slice(&[0]);
    return sep;
}

pub fn find_short_succ(a: &[u8]) -> Vec<u8> {
    let mut result = a.to_vec();
    for i in 0..a.len() {
        if a[i] != 0xff {
            result[i] += 1;
            result.resize(i + 1, 0);
            return result;
        }
    }
    // Rare path
    result.push(255);
    return result;
}

const MASK_DELTA: u32 = 0xa282ead8;

pub fn mask_crc(c: u32) -> u32 {
    (c.wrapping_shr(15) | c.wrapping_shl(17)).wrapping_add(MASK_DELTA)
}

pub fn unmask_crc(mc: u32) -> u32 {
    let rot = mc.wrapping_sub(MASK_DELTA);
    (rot.wrapping_shr(17) | rot.wrapping_shl(15))
}

pub fn read_unlock<T>(l: &RwLock<T>) -> RwLockReadGuard<T> {
    match l.read() {
        Ok(v) => v,
        Err(poised) => poised.into_inner(),
    }
}

pub fn write_unlock<T>(l: &RwLock<T>) -> RwLockWriteGuard<T> {
    match l.write() {
        Ok(v) => v,
        Err(poised) => poised.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block_builder::BLOCK_CKSUM_LEN;

    #[test]
    fn test_crc() {
        let c = ::std::u32::MAX;
        let n = mask_crc(c);
        assert_eq!(BLOCK_CKSUM_LEN, ::std::mem::size_of_val(&n));
        assert_ne!(c, n);
        assert_eq!(c, unmask_crc(n));
    }

    #[test]
    fn test_find_shortest_sep() {
        assert_eq!(
            find_shortest_sep("abcd".as_bytes(), "abcf".as_bytes()),
            "abce".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("abc".as_bytes(), "acd".as_bytes()),
            "abc\0".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("abcdefghi".as_bytes(), "abcffghi".as_bytes()),
            "abce".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("a".as_bytes(), "a".as_bytes()),
            "a".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("a".as_bytes(), "b".as_bytes()),
            "a\0".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("abc".as_bytes(), "zzz".as_bytes()),
            "b".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("yyy".as_bytes(), "z".as_bytes()),
            "yyy\0".as_bytes()
        );
        assert_eq!(
            find_shortest_sep("".as_bytes(), "".as_bytes()),
            "".as_bytes()
        );
    }

    #[test]
    fn test_find_short_succ() {
        assert_eq!(find_short_succ("abcd".as_bytes()), "b".as_bytes());
        assert_eq!(find_short_succ("zzzz".as_bytes()), "{".as_bytes());
        assert_eq!(find_short_succ(&[]), &[0xff]);
        assert_eq!(
            find_short_succ(&[0xff, 0xff, 0xff]),
            &[0xff, 0xff, 0xff, 0xff]
        );
    }
}
