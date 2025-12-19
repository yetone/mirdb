import { describe, it, expect } from 'vitest';
import { tokenize, isSupportedLanguage, getTokenClassName, type Token } from './syntaxHighlighter';

describe('syntaxHighlighter', () => {
  describe('isSupportedLanguage', () => {
    it('returns true for bash', () => {
      expect(isSupportedLanguage('bash')).toBe(true);
    });

    it('returns true for shell', () => {
      expect(isSupportedLanguage('shell')).toBe(true);
    });

    it('returns true for toml', () => {
      expect(isSupportedLanguage('toml')).toBe(true);
    });

    it('returns true for rust', () => {
      expect(isSupportedLanguage('rust')).toBe(true);
    });

    it('returns false for unsupported languages', () => {
      expect(isSupportedLanguage('python')).toBe(false);
      expect(isSupportedLanguage('javascript')).toBe(false);
      expect(isSupportedLanguage('unknown')).toBe(false);
    });
  });

  describe('getTokenClassName', () => {
    it('returns correct class names for token types', () => {
      expect(getTokenClassName('keyword')).toBe('syntax-keyword');
      expect(getTokenClassName('string')).toBe('syntax-string');
      expect(getTokenClassName('comment')).toBe('syntax-comment');
      expect(getTokenClassName('number')).toBe('syntax-number');
      expect(getTokenClassName('operator')).toBe('syntax-operator');
      expect(getTokenClassName('function')).toBe('syntax-function');
      expect(getTokenClassName('variable')).toBe('syntax-variable');
      expect(getTokenClassName('property')).toBe('syntax-property');
      expect(getTokenClassName('punctuation')).toBe('syntax-punctuation');
      expect(getTokenClassName('builtin')).toBe('syntax-builtin');
      expect(getTokenClassName('plain')).toBe('syntax-plain');
    });
  });

  describe('tokenize - Bash/Shell', () => {
    it('tokenizes simple bash command', () => {
      const tokens = tokenize('cargo install mirdb', 'bash');
      expect(tokens.length).toBeGreaterThan(0);

      // Find the builtin 'cargo'
      const cargoToken = tokens.find(t => t.value === 'cargo');
      expect(cargoToken).toBeDefined();
      expect(cargoToken?.type).toBe('builtin');
    });

    it('tokenizes bash comments', () => {
      const tokens = tokenize('# This is a comment', 'bash');
      expect(tokens.length).toBe(1);
      expect(tokens[0].type).toBe('comment');
      expect(tokens[0].value).toBe('# This is a comment');
    });

    it('tokenizes bash strings', () => {
      const tokens = tokenize('echo "hello world"', 'bash');
      const stringToken = tokens.find(t => t.type === 'string');
      expect(stringToken).toBeDefined();
      expect(stringToken?.value).toBe('"hello world"');
    });

    it('tokenizes bash variables', () => {
      const tokens = tokenize('echo $HOME', 'bash');
      const varToken = tokens.find(t => t.type === 'variable');
      expect(varToken).toBeDefined();
      expect(varToken?.value).toBe('$HOME');
    });

    it('tokenizes bash operators', () => {
      const tokens = tokenize('cmd1 | cmd2', 'bash');
      const opToken = tokens.find(t => t.type === 'operator');
      expect(opToken).toBeDefined();
      expect(opToken?.value).toBe('|');
    });

    it('tokenizes complex bash command', () => {
      const code = `# Install and run MirDB
cargo install mirdb
mirdb -c mirdb.toml`;
      const tokens = tokenize(code, 'bash');

      // Should have a comment
      const commentToken = tokens.find(t => t.type === 'comment');
      expect(commentToken).toBeDefined();

      // Should have at least one builtin command (cargo)
      const builtins = tokens.filter(t => t.type === 'builtin');
      expect(builtins.length).toBeGreaterThanOrEqual(1);
    });

    it('tokenizes bash keywords', () => {
      const tokens = tokenize('if [ true ]; then echo "yes"; fi', 'bash');
      const ifToken = tokens.find(t => t.value === 'if');
      const thenToken = tokens.find(t => t.value === 'then');
      const fiToken = tokens.find(t => t.value === 'fi');

      expect(ifToken?.type).toBe('keyword');
      expect(thenToken?.type).toBe('keyword');
      expect(fiToken?.type).toBe('keyword');
    });

    it('recognizes telnet as builtin', () => {
      const tokens = tokenize('telnet localhost 12333', 'bash');
      const telnetToken = tokens.find(t => t.value === 'telnet');
      expect(telnetToken?.type).toBe('builtin');
    });

    it('recognizes set and get as builtins for memcached commands', () => {
      const tokens = tokenize('set mykey 0 0 5', 'bash');
      const setToken = tokens.find(t => t.value === 'set');
      expect(setToken?.type).toBe('builtin');

      const getTokens = tokenize('get mykey', 'bash');
      const getToken = getTokens.find(t => t.value === 'get');
      expect(getToken?.type).toBe('builtin');
    });
  });

  describe('tokenize - TOML', () => {
    it('tokenizes TOML key-value pairs', () => {
      const tokens = tokenize('addr = "0.0.0.0:12333"', 'toml');

      // Should have property (key)
      const propToken = tokens.find(t => t.type === 'property');
      expect(propToken).toBeDefined();
      expect(propToken?.value).toBe('addr');

      // Should have operator
      const opToken = tokens.find(t => t.type === 'operator');
      expect(opToken).toBeDefined();
      expect(opToken?.value).toBe('=');

      // Should have string
      const stringToken = tokens.find(t => t.type === 'string');
      expect(stringToken).toBeDefined();
      expect(stringToken?.value).toBe('"0.0.0.0:12333"');
    });

    it('tokenizes TOML numbers', () => {
      const tokens = tokenize('max_level = 7', 'toml');
      const numToken = tokens.find(t => t.type === 'number');
      expect(numToken).toBeDefined();
      expect(numToken?.value).toBe('7');
    });

    it('tokenizes TOML section headers', () => {
      const tokens = tokenize('[server]', 'toml');
      const headerToken = tokens.find(t => t.type === 'keyword');
      expect(headerToken).toBeDefined();
      expect(headerToken?.value).toBe('[server]');
    });

    it('tokenizes TOML comments', () => {
      const tokens = tokenize('# Configuration file', 'toml');
      const commentToken = tokens.find(t => t.type === 'comment');
      expect(commentToken).toBeDefined();
      expect(commentToken?.value).toBe('# Configuration file');
    });

    it('tokenizes TOML boolean values', () => {
      const tokens = tokenize('enabled = true', 'toml');
      const boolToken = tokens.find(t => t.value === 'true');
      expect(boolToken?.type).toBe('keyword');
    });

    it('tokenizes complete TOML config', () => {
      const config = `addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"`;

      const tokens = tokenize(config, 'toml');

      // Count properties (keys)
      const props = tokens.filter(t => t.type === 'property');
      expect(props.length).toBe(5);

      // Count strings
      const strings = tokens.filter(t => t.type === 'string');
      expect(strings.length).toBe(4); // addr, work_dir, sst_max_size, mem_table_max_size

      // Count numbers
      const numbers = tokens.filter(t => t.type === 'number');
      expect(numbers.length).toBe(1); // max_level = 7
    });
  });

  describe('tokenize - Rust', () => {
    it('tokenizes Rust keywords', () => {
      const tokens = tokenize('let mut x = 5;', 'rust');

      const letToken = tokens.find(t => t.value === 'let');
      const mutToken = tokens.find(t => t.value === 'mut');

      expect(letToken?.type).toBe('keyword');
      expect(mutToken?.type).toBe('keyword');
    });

    it('tokenizes Rust functions/macros', () => {
      const tokens = tokenize('println!("Hello")', 'rust');
      const macroToken = tokens.find(t => t.value === 'println!');
      expect(macroToken?.type).toBe('function');
    });

    it('tokenizes Rust strings', () => {
      const tokens = tokenize('"hello world"', 'rust');
      const stringToken = tokens.find(t => t.type === 'string');
      expect(stringToken).toBeDefined();
      expect(stringToken?.value).toBe('"hello world"');
    });

    it('tokenizes Rust comments', () => {
      const tokens = tokenize('// This is a comment', 'rust');
      const commentToken = tokens.find(t => t.type === 'comment');
      expect(commentToken).toBeDefined();
      expect(commentToken?.value).toBe('// This is a comment');
    });

    it('tokenizes Rust builtin types', () => {
      const tokens = tokenize('let s: String = String::new();', 'rust');
      const stringTypes = tokens.filter(t => t.value === 'String');
      stringTypes.forEach(t => {
        expect(t.type).toBe('builtin');
      });
    });

    it('tokenizes Rust numbers', () => {
      const tokens = tokenize('let x = 42;', 'rust');
      const numToken = tokens.find(t => t.type === 'number');
      expect(numToken).toBeDefined();
      expect(numToken?.value).toBe('42');
    });

    it('tokenizes Rust operators', () => {
      const tokens = tokenize('x + y * z', 'rust');
      const ops = tokens.filter(t => t.type === 'operator');
      expect(ops.length).toBe(2);
    });

    it('tokenizes Rust lifetime annotations', () => {
      const tokens = tokenize("fn foo<'a>(x: &'a str) {}", 'rust');
      const lifetimes = tokens.filter(t => t.value.startsWith("'") && t.type === 'variable');
      expect(lifetimes.length).toBeGreaterThanOrEqual(1);
    });

    it('tokenizes fn keyword', () => {
      const tokens = tokenize('fn main() {}', 'rust');
      const fnToken = tokens.find(t => t.value === 'fn');
      expect(fnToken?.type).toBe('keyword');
    });

    it('tokenizes struct keyword', () => {
      const tokens = tokenize('struct Point { x: i32, y: i32 }', 'rust');
      const structToken = tokens.find(t => t.value === 'struct');
      expect(structToken?.type).toBe('keyword');
    });
  });

  describe('tokenize - Unsupported languages', () => {
    it('returns plain token for unsupported language', () => {
      const code = 'some random code';
      const tokens = tokenize(code, 'python');
      expect(tokens.length).toBe(1);
      expect(tokens[0].type).toBe('plain');
      expect(tokens[0].value).toBe(code);
    });
  });

  describe('tokenize - preserves original code', () => {
    it('joining tokens reconstructs original bash code', () => {
      const code = 'cargo install mirdb';
      const tokens = tokenize(code, 'bash');
      const reconstructed = tokens.map(t => t.value).join('');
      expect(reconstructed).toBe(code);
    });

    it('joining tokens reconstructs original TOML code', () => {
      const code = `addr = "0.0.0.0:12333"
max_level = 7`;
      const tokens = tokenize(code, 'toml');
      const reconstructed = tokens.map(t => t.value).join('');
      expect(reconstructed).toBe(code);
    });

    it('joining tokens reconstructs original Rust code', () => {
      const code = 'fn main() { println!("Hello"); }';
      const tokens = tokenize(code, 'rust');
      const reconstructed = tokens.map(t => t.value).join('');
      expect(reconstructed).toBe(code);
    });
  });
});
