pub mod command;

use std::str;
pub use self::command::Command;

fn to_str(cs: &[u8]) -> &str {
    str::from_utf8(&cs).expect("not a valid utf8")
}

pub fn parse<'a>(cs: &'a [u8]) -> Command<'a> {
    let mut tokens: Vec<&'a [u8]> = vec![];
    let mut start = 0;
    let mut end = 0;
    let mut quotes_count = 0;
    for c in cs {
        end += 1;
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
        if c == '\r' {
            if end < cs.len() && cs[end] as char == '\n' {
                if start < end - 1 {
                    tokens.push(&cs[start..end - 1]);
                }
                start = end - 1;
                end += 1;
                break;
            }
        }
    }

    if quotes_count > 0 {
        return Command::Error;
    }

    if end > start {
        tokens.push(&cs[start..end]);
    }

    if tokens.len() < 2 {
        return Command::Error;
    }

    if to_str(tokens[tokens.len() - 1]) != "\r\n" {
        return Command::Error;
    }

    tokens.pop();

    let cmd = tokens[0];

    match to_str(cmd) {
        "get" => {
            if tokens.len() != 2 {
                return Command::Error;
            }
            return Command::Getter {
                key: tokens[1],
            };
        }
        "set" => {
            if tokens.len() != 3 {
                return Command::Error;
            }
            return Command::Setter {
                key: tokens[1],
                value: tokens[2]
            };
        }
        _ => Command::Error
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
        assert_eq!(parse(b"set abc \"a b c\"\r\n"), Command::Setter {
            key: b"abc",
            value: b"a b c",
        });
    }
}
