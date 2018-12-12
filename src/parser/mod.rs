pub mod command;

use crate::utils::to_str;
pub use self::command::Command;

macro_rules! try_cmd {
    ($e:expr, $err:expr) => {
        match $e {
            Ok(v) => v,
            _ => return Command::Error($err),
        }
    }
}

pub fn parse<'a>(cs: &'a [u8]) -> Command<'a> {
    let mut tokens: Vec<&'a [u8]> = vec![];
    let mut start = 0;
    let mut end = 0;
    let mut quotes_count = 0;
    let mut hold = false;
    for c in cs {
        end += 1;
        if hold {
            continue;
        }
        let c = *c as char;
        if quotes_count > 0 {
            if c == '"' && end > 1 && cs[end - 2] as char != '\\' {
                tokens.push(&cs[start + 1..end - 1]);
                start = end;
                quotes_count -= 1;
            }
            continue;
        }
        if c == '"' {
            quotes_count += 1;
            continue;
        }
        if c == ' ' {
            tokens.push(&cs[start..end - 1]);
            start = end;
            continue;
        }
        if c == '\n' {
            if end > 1 && cs[end - 2] as char == '\r' {
                if start < end - 2 {
                    tokens.push(&cs[start..end - 2]);
                }
                tokens.push(&cs[end - 2..end]);
                if !hold && to_str(tokens[0]) == "set" && tokens.len() == 6 || tokens.len() == 7 {
                    hold = true;
                }
                start = end;
            }
        }
    }

    if end > start {
        tokens.push(&cs[start..end]);
    }

    if quotes_count != 0 {
        return Command::Incomplete;
    }

    if tokens.len() < 2 {
        return Command::Incomplete;
    }

    if hold {
        let last = tokens.pop().unwrap();
        if last.len() < 2 {
            return Command::Incomplete;
        }
        if to_str(&last[last.len() - 2..last.len()]) != "\r\n" {
            return Command::Incomplete;
        }
        tokens.push(&last[..last.len() - 2]);
        tokens.push(&last[last.len() - 2..]);
    }

    if to_str(tokens[tokens.len() - 1]) != "\r\n" {
        return Command::Incomplete;
    }

    let cmd = tokens[0];
    let l = tokens.len();

    match to_str(cmd) {
        "get" => {
            if l != 3 {
                return Command::Error("ARG COUNT ERROR");
            }
            return Command::Getter {
                key: tokens[1],
            };
        }
        "set" => {
            if l != 8 && l != 9 {
                if l < 8 {
                    return Command::Incomplete;
                }
                return Command::Error("ARG COUNT ERROR");
            }
            let key = tokens[1];
            let flags = try_cmd!(to_str(tokens[2]).parse::<u32>(), "flags is not int");
            let ttl = try_cmd!(to_str(tokens[3]).parse::<u32>(), "ttl is not int");
            let bytes = try_cmd!(to_str(tokens[4]).parse::<usize>(), "bytes is not int");
            let noreply = if l == 8 {false} else {to_str(tokens[5]) == "noreply"};
            let payload = if l == 8 {tokens[6]} else {tokens[7]};
            if payload.len() < bytes {
                return Command::Incomplete;
            }
            return Command::Setter {
                key,
                flags,
                ttl,
                bytes,
                noreply,
                payload
            };
        }
        _ => Command::Error("ERROR")
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {
        use super::parse;
        use super::command::Command;
        assert_eq!(parse(b"get abc\r\n"), Command::Getter {
            key: b"abc",
        });
        assert_eq!(parse(b"get \"a b c\"\r\n"), Command::Getter {
            key: b"a b c",
        });
        assert_eq!(parse(b"set abc 1 0 5\r\n\"a b c\"\r\n"), Command::Setter {
            key: b"abc",
            flags: 1,
            ttl: 0,
            bytes: 5,
            noreply: false,
            payload: b"\"a b c\"",
        });
    }
}
