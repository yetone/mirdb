/**
 * Lightweight syntax highlighting for code blocks
 * Supports: bash/shell, TOML, and Rust
 */

export interface Token {
  type: 'keyword' | 'string' | 'comment' | 'number' | 'operator' | 'function' | 'variable' | 'property' | 'punctuation' | 'builtin' | 'plain';
  value: string;
}

export type SupportedLanguage = 'bash' | 'shell' | 'toml' | 'rust';

/**
 * Check if the given language is supported for syntax highlighting
 */
export function isSupportedLanguage(language: string): language is SupportedLanguage {
  return ['bash', 'shell', 'toml', 'rust'].includes(language.toLowerCase());
}

/**
 * Tokenize bash/shell code
 */
function tokenizeBash(code: string): Token[] {
  const tokens: Token[] = [];
  const keywords = ['if', 'then', 'else', 'elif', 'fi', 'for', 'while', 'do', 'done', 'case', 'esac', 'in', 'function', 'return', 'exit', 'export', 'local', 'readonly', 'declare', 'typeset'];
  const builtins = ['echo', 'cd', 'pwd', 'ls', 'cat', 'grep', 'sed', 'awk', 'curl', 'wget', 'tar', 'make', 'git', 'cargo', 'npm', 'node', 'python', 'pip', 'telnet', 'set', 'get', 'delete', 'source', 'alias', 'unalias'];

  let i = 0;
  while (i < code.length) {
    // Comments
    if (code[i] === '#' && (i === 0 || code[i - 1] === '\n' || code[i - 1] === ' ' || code[i - 1] === '\t')) {
      let comment = '';
      while (i < code.length && code[i] !== '\n') {
        comment += code[i];
        i++;
      }
      tokens.push({ type: 'comment', value: comment });
      continue;
    }

    // Double-quoted strings
    if (code[i] === '"') {
      let str = '"';
      i++;
      while (i < code.length && code[i] !== '"') {
        if (code[i] === '\\' && i + 1 < code.length) {
          str += code[i] + code[i + 1];
          i += 2;
        } else {
          str += code[i];
          i++;
        }
      }
      if (i < code.length) {
        str += '"';
        i++;
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Single-quoted strings
    if (code[i] === "'") {
      let str = "'";
      i++;
      while (i < code.length && code[i] !== "'") {
        str += code[i];
        i++;
      }
      if (i < code.length) {
        str += "'";
        i++;
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Variables ($VAR, ${VAR})
    if (code[i] === '$') {
      let variable = '$';
      i++;
      if (i < code.length && code[i] === '{') {
        while (i < code.length && code[i] !== '}') {
          variable += code[i];
          i++;
        }
        if (i < code.length) {
          variable += '}';
          i++;
        }
      } else {
        while (i < code.length && /[a-zA-Z0-9_]/.test(code[i])) {
          variable += code[i];
          i++;
        }
      }
      tokens.push({ type: 'variable', value: variable });
      continue;
    }

    // Operators
    if (['|', '&', '>', '<', ';', '='].includes(code[i])) {
      let op = code[i];
      i++;
      // Handle multi-character operators
      if (i < code.length && (code[i] === '|' || code[i] === '&' || code[i] === '>' || code[i] === '=')) {
        op += code[i];
        i++;
      }
      tokens.push({ type: 'operator', value: op });
      continue;
    }

    // Numbers
    if (/[0-9]/.test(code[i])) {
      let num = '';
      while (i < code.length && /[0-9]/.test(code[i])) {
        num += code[i];
        i++;
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }

    // Words (keywords, builtins, identifiers)
    if (/[a-zA-Z_]/.test(code[i])) {
      let word = '';
      while (i < code.length && /[a-zA-Z0-9_-]/.test(code[i])) {
        word += code[i];
        i++;
      }
      if (keywords.includes(word.toLowerCase())) {
        tokens.push({ type: 'keyword', value: word });
      } else if (builtins.includes(word.toLowerCase())) {
        tokens.push({ type: 'builtin', value: word });
      } else {
        tokens.push({ type: 'plain', value: word });
      }
      continue;
    }

    // Punctuation and other characters
    tokens.push({ type: 'plain', value: code[i] });
    i++;
  }

  return tokens;
}

/**
 * Tokenize TOML code
 */
function tokenizeToml(code: string): Token[] {
  const tokens: Token[] = [];

  let i = 0;
  while (i < code.length) {
    // Comments
    if (code[i] === '#') {
      let comment = '';
      while (i < code.length && code[i] !== '\n') {
        comment += code[i];
        i++;
      }
      tokens.push({ type: 'comment', value: comment });
      continue;
    }

    // Section headers [section] or [[array]]
    if (code[i] === '[') {
      let section = '[';
      i++;
      if (i < code.length && code[i] === '[') {
        section += '[';
        i++;
      }
      while (i < code.length && code[i] !== ']') {
        section += code[i];
        i++;
      }
      while (i < code.length && code[i] === ']') {
        section += ']';
        i++;
      }
      tokens.push({ type: 'keyword', value: section });
      continue;
    }

    // Double-quoted strings
    if (code[i] === '"') {
      let str = '"';
      i++;
      // Check for triple quotes
      if (i + 1 < code.length && code[i] === '"' && code[i + 1] === '"') {
        str += '""';
        i += 2;
        while (i + 2 < code.length && !(code[i] === '"' && code[i + 1] === '"' && code[i + 2] === '"')) {
          str += code[i];
          i++;
        }
        if (i + 2 < code.length) {
          str += '"""';
          i += 3;
        }
      } else {
        while (i < code.length && code[i] !== '"' && code[i] !== '\n') {
          if (code[i] === '\\' && i + 1 < code.length) {
            str += code[i] + code[i + 1];
            i += 2;
          } else {
            str += code[i];
            i++;
          }
        }
        if (i < code.length && code[i] === '"') {
          str += '"';
          i++;
        }
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Single-quoted strings (literal)
    if (code[i] === "'") {
      let str = "'";
      i++;
      // Check for triple quotes
      if (i + 1 < code.length && code[i] === "'" && code[i + 1] === "'") {
        str += "''";
        i += 2;
        while (i + 2 < code.length && !(code[i] === "'" && code[i + 1] === "'" && code[i + 2] === "'")) {
          str += code[i];
          i++;
        }
        if (i + 2 < code.length) {
          str += "'''";
          i += 3;
        }
      } else {
        while (i < code.length && code[i] !== "'" && code[i] !== '\n') {
          str += code[i];
          i++;
        }
        if (i < code.length && code[i] === "'") {
          str += "'";
          i++;
        }
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Key names (before =)
    if (/[a-zA-Z_]/.test(code[i])) {
      let key = '';
      const startI = i;
      while (i < code.length && /[a-zA-Z0-9_-]/.test(code[i])) {
        key += code[i];
        i++;
      }
      // Check if this is a boolean or null value
      if (['true', 'false'].includes(key.toLowerCase())) {
        tokens.push({ type: 'keyword', value: key });
        continue;
      }
      // Look ahead to see if there's an = sign (skip whitespace)
      let lookAhead = i;
      while (lookAhead < code.length && (code[lookAhead] === ' ' || code[lookAhead] === '\t')) {
        lookAhead++;
      }
      if (lookAhead < code.length && code[lookAhead] === '=') {
        tokens.push({ type: 'property', value: key });
      } else {
        tokens.push({ type: 'plain', value: key });
      }
      continue;
    }

    // Numbers (integers, floats, dates)
    if (/[0-9]/.test(code[i]) || (code[i] === '-' && i + 1 < code.length && /[0-9]/.test(code[i + 1]))) {
      let num = '';
      if (code[i] === '-') {
        num += '-';
        i++;
      }
      while (i < code.length && /[0-9eE._:TZ+-]/.test(code[i])) {
        num += code[i];
        i++;
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }

    // Operator (=)
    if (code[i] === '=') {
      tokens.push({ type: 'operator', value: '=' });
      i++;
      continue;
    }

    // Punctuation
    if ([',', '{', '}'].includes(code[i])) {
      tokens.push({ type: 'punctuation', value: code[i] });
      i++;
      continue;
    }

    // Other characters (whitespace, etc.)
    tokens.push({ type: 'plain', value: code[i] });
    i++;
  }

  return tokens;
}

/**
 * Tokenize Rust code
 */
function tokenizeRust(code: string): Token[] {
  const tokens: Token[] = [];
  const keywords = ['as', 'async', 'await', 'break', 'const', 'continue', 'crate', 'dyn', 'else', 'enum', 'extern', 'false', 'fn', 'for', 'if', 'impl', 'in', 'let', 'loop', 'match', 'mod', 'move', 'mut', 'pub', 'ref', 'return', 'self', 'Self', 'static', 'struct', 'super', 'trait', 'true', 'type', 'unsafe', 'use', 'where', 'while'];
  const builtins = ['String', 'Vec', 'Option', 'Result', 'Box', 'Rc', 'Arc', 'Cell', 'RefCell', 'HashMap', 'HashSet', 'BTreeMap', 'BTreeSet', 'Ok', 'Err', 'Some', 'None', 'println', 'print', 'eprintln', 'eprint', 'format', 'panic', 'assert', 'debug_assert', 'todo', 'unimplemented'];

  let i = 0;
  while (i < code.length) {
    // Line comments
    if (code[i] === '/' && i + 1 < code.length && code[i + 1] === '/') {
      let comment = '';
      while (i < code.length && code[i] !== '\n') {
        comment += code[i];
        i++;
      }
      tokens.push({ type: 'comment', value: comment });
      continue;
    }

    // Block comments
    if (code[i] === '/' && i + 1 < code.length && code[i + 1] === '*') {
      let comment = '/*';
      i += 2;
      while (i + 1 < code.length && !(code[i] === '*' && code[i + 1] === '/')) {
        comment += code[i];
        i++;
      }
      if (i + 1 < code.length) {
        comment += '*/';
        i += 2;
      }
      tokens.push({ type: 'comment', value: comment });
      continue;
    }

    // String literals
    if (code[i] === '"') {
      let str = '"';
      i++;
      while (i < code.length && code[i] !== '"') {
        if (code[i] === '\\' && i + 1 < code.length) {
          str += code[i] + code[i + 1];
          i += 2;
        } else {
          str += code[i];
          i++;
        }
      }
      if (i < code.length) {
        str += '"';
        i++;
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Raw strings
    if (code[i] === 'r' && i + 1 < code.length && (code[i + 1] === '"' || code[i + 1] === '#')) {
      let str = 'r';
      i++;
      let hashes = 0;
      while (i < code.length && code[i] === '#') {
        str += '#';
        hashes++;
        i++;
      }
      if (i < code.length && code[i] === '"') {
        str += '"';
        i++;
        let endFound = false;
        while (i < code.length && !endFound) {
          if (code[i] === '"') {
            str += '"';
            i++;
            let endHashes = 0;
            while (i < code.length && endHashes < hashes && code[i] === '#') {
              str += '#';
              endHashes++;
              i++;
            }
            if (endHashes === hashes) {
              endFound = true;
            }
          } else {
            str += code[i];
            i++;
          }
        }
      }
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Character literals
    if (code[i] === "'" && (i + 1 >= code.length || !/[a-zA-Z_]/.test(code[i + 1]) || (i + 2 < code.length && (code[i + 2] === "'" || code[i + 1] === '\\')))) {
      let char = "'";
      i++;
      if (i < code.length) {
        if (code[i] === '\\' && i + 1 < code.length) {
          char += code[i] + code[i + 1];
          i += 2;
        } else if (code[i] !== "'") {
          char += code[i];
          i++;
        }
      }
      if (i < code.length && code[i] === "'") {
        char += "'";
        i++;
        tokens.push({ type: 'string', value: char });
        continue;
      } else {
        // Not a char literal, backtrack
        tokens.push({ type: 'plain', value: char });
        continue;
      }
    }

    // Lifetime annotations
    if (code[i] === "'" && i + 1 < code.length && /[a-zA-Z_]/.test(code[i + 1])) {
      let lifetime = "'";
      i++;
      while (i < code.length && /[a-zA-Z0-9_]/.test(code[i])) {
        lifetime += code[i];
        i++;
      }
      tokens.push({ type: 'variable', value: lifetime });
      continue;
    }

    // Attributes #[...]
    if (code[i] === '#' && i + 1 < code.length && code[i + 1] === '[') {
      let attr = '#[';
      i += 2;
      let depth = 1;
      while (i < code.length && depth > 0) {
        if (code[i] === '[') depth++;
        if (code[i] === ']') depth--;
        attr += code[i];
        i++;
      }
      tokens.push({ type: 'keyword', value: attr });
      continue;
    }

    // Macros (word followed by !)
    if (/[a-zA-Z_]/.test(code[i])) {
      let word = '';
      const startI = i;
      while (i < code.length && /[a-zA-Z0-9_]/.test(code[i])) {
        word += code[i];
        i++;
      }
      // Check if it's a macro
      if (i < code.length && code[i] === '!') {
        word += '!';
        i++;
        tokens.push({ type: 'function', value: word });
        continue;
      }
      // Check if it's a keyword
      if (keywords.includes(word)) {
        tokens.push({ type: 'keyword', value: word });
        continue;
      }
      // Check if it's a builtin type/function
      if (builtins.includes(word)) {
        tokens.push({ type: 'builtin', value: word });
        continue;
      }
      // Check if it looks like a type (PascalCase)
      if (/^[A-Z][a-zA-Z0-9]*$/.test(word)) {
        tokens.push({ type: 'builtin', value: word });
        continue;
      }
      tokens.push({ type: 'plain', value: word });
      continue;
    }

    // Numbers
    if (/[0-9]/.test(code[i])) {
      let num = '';
      // Hex, octal, binary
      if (code[i] === '0' && i + 1 < code.length) {
        if (code[i + 1] === 'x' || code[i + 1] === 'X') {
          num += code[i] + code[i + 1];
          i += 2;
          while (i < code.length && /[0-9a-fA-F_]/.test(code[i])) {
            num += code[i];
            i++;
          }
        } else if (code[i + 1] === 'o' || code[i + 1] === 'O') {
          num += code[i] + code[i + 1];
          i += 2;
          while (i < code.length && /[0-7_]/.test(code[i])) {
            num += code[i];
            i++;
          }
        } else if (code[i + 1] === 'b' || code[i + 1] === 'B') {
          num += code[i] + code[i + 1];
          i += 2;
          while (i < code.length && /[01_]/.test(code[i])) {
            num += code[i];
            i++;
          }
        }
      }
      if (!num) {
        while (i < code.length && /[0-9._eE+-]/.test(code[i])) {
          num += code[i];
          i++;
        }
      }
      // Type suffix
      while (i < code.length && /[a-zA-Z0-9_]/.test(code[i])) {
        num += code[i];
        i++;
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }

    // Operators
    if (['+', '-', '*', '/', '%', '=', '!', '<', '>', '&', '|', '^', '~', '?', ':', '.', '@'].includes(code[i])) {
      let op = code[i];
      i++;
      // Handle multi-character operators
      while (i < code.length && ['+', '-', '*', '/', '%', '=', '!', '<', '>', '&', '|', '^', '.'].includes(code[i])) {
        op += code[i];
        i++;
      }
      tokens.push({ type: 'operator', value: op });
      continue;
    }

    // Punctuation
    if ([',', ';', '(', ')', '{', '}', '[', ']'].includes(code[i])) {
      tokens.push({ type: 'punctuation', value: code[i] });
      i++;
      continue;
    }

    // Other characters
    tokens.push({ type: 'plain', value: code[i] });
    i++;
  }

  return tokens;
}

/**
 * Tokenize code based on language
 */
export function tokenize(code: string, language: string): Token[] {
  const lang = language.toLowerCase();

  switch (lang) {
    case 'bash':
    case 'shell':
    case 'sh':
      return tokenizeBash(code);
    case 'toml':
      return tokenizeToml(code);
    case 'rust':
    case 'rs':
      return tokenizeRust(code);
    default:
      // Return the entire code as a single plain token for unsupported languages
      return [{ type: 'plain', value: code }];
  }
}

/**
 * Get CSS class for a token type
 */
export function getTokenClassName(type: Token['type']): string {
  return `syntax-${type}`;
}
