/**
 * Syntax Highlighter Utility.
 * Owner: Scenario 3 - Usage Examples with Syntax Highlighting
 *
 * Provides syntax highlighting for memcached protocol commands.
 * Lightweight implementation without external dependencies.
 *
 * Expected exports:
 * - highlightCode(code: string, language: string): string
 * - createCodeBlock(code: string, language: string): HTMLElement
 */

export interface HighlightToken {
  type: 'command' | 'key' | 'value' | 'number' | 'response' | 'text' | 'newline';
  value: string;
}

/**
 * Tokenizes memcached protocol code for syntax highlighting.
 */
export function tokenizeMemcached(code: string): HighlightToken[] {
  const tokens: HighlightToken[] = [];
  const lines = code.split(/(\r?\n)/);

  const commands = ['set', 'get', 'delete', 'add', 'replace', 'append', 'prepend', 'gets', 'info', 'major_compaction'];
  const responseKeywords = ['STORED', 'NOT_STORED', 'EXISTS', 'NOT_FOUND', 'DELETED', 'VALUE', 'END', 'ERROR'];

  for (const line of lines) {
    if (line === '\n' || line === '\r\n') {
      tokens.push({ type: 'newline', value: line });
      continue;
    }

    const parts = line.split(/(\s+)/);
    let isFirstWord = true;
    let expectingKey = false;
    let expectingNumbers = false;
    let isValueLine = false;

    // Check if this line starts with a response keyword
    const firstWord = parts.find(p => p.trim().length > 0);
    if (firstWord && responseKeywords.includes(firstWord.toUpperCase())) {
      isValueLine = true;
    }

    for (const part of parts) {
      if (!part) continue;

      // Whitespace
      if (/^\s+$/.test(part)) {
        tokens.push({ type: 'text', value: part });
        continue;
      }

      // Response keywords
      if (responseKeywords.includes(part.toUpperCase())) {
        tokens.push({ type: 'response', value: part });
        expectingKey = part.toUpperCase() === 'VALUE';
        expectingNumbers = false;
        isFirstWord = false;
        continue;
      }

      // Commands
      if (isFirstWord && commands.includes(part.toLowerCase())) {
        tokens.push({ type: 'command', value: part });
        expectingKey = true;
        isFirstWord = false;
        continue;
      }

      // Numbers (flags, ttl, bytes)
      if (/^\d+$/.test(part)) {
        tokens.push({ type: 'number', value: part });
        continue;
      }

      // Keys (after command or VALUE response)
      if (expectingKey) {
        tokens.push({ type: 'key', value: part });
        expectingKey = false;
        expectingNumbers = true;
        continue;
      }

      // Values (data lines or non-command text)
      if (isValueLine && !responseKeywords.includes(part.toUpperCase())) {
        tokens.push({ type: 'value', value: part });
        continue;
      }

      // Default to value for data lines
      if (!isFirstWord && !expectingNumbers) {
        tokens.push({ type: 'value', value: part });
      } else {
        tokens.push({ type: 'text', value: part });
      }

      isFirstWord = false;
    }
  }

  return tokens;
}

/**
 * Highlights code and returns HTML string with syntax highlighting spans.
 */
export function highlightCode(code: string, language: string): string {
  if (language !== 'memcached') {
    return escapeHtml(code);
  }

  const tokens = tokenizeMemcached(code);
  let html = '';

  for (const token of tokens) {
    if (token.type === 'newline') {
      html += '\n';
    } else if (token.type === 'text') {
      html += escapeHtml(token.value);
    } else {
      html += `<span class="syntax-${token.type}">${escapeHtml(token.value)}</span>`;
    }
  }

  return html;
}

/**
 * Creates an HTMLElement code block with syntax highlighting.
 */
export function createCodeBlock(code: string, language: string): HTMLElement {
  const pre = document.createElement('pre');
  pre.className = 'code-block';
  pre.setAttribute('data-language', language);

  const codeEl = document.createElement('code');
  codeEl.className = 'code-content';
  codeEl.innerHTML = highlightCode(code, language);

  pre.appendChild(codeEl);
  return pre;
}

/**
 * Escapes HTML special characters to prevent XSS.
 */
function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}
