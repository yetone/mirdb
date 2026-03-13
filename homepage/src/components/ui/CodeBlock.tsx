/**
 * Code Block Component
 * Owner: Scenario 5 - Quick Start Section
 *
 * Expected exports:
 * - CodeBlock: Syntax highlighted code display with copy button
 *
 * Props:
 * - code: string
 * - language?: string
 * - showCopyButton?: boolean
 */

import { useCopyToClipboard } from '../../hooks/useCopyToClipboard';

interface CodeBlockProps {
  code: string;
  language?: string;
  showCopyButton?: boolean;
}

interface Token {
  type: 'prompt' | 'command' | 'flag' | 'value' | 'comment' | 'text';
  value: string;
}

function tokenizeShell(code: string): Token[] {
  const tokens: Token[] = [];
  const lines = code.split('\n');

  for (const line of lines) {
    if (line.startsWith('$')) {
      tokens.push({ type: 'prompt', value: '$ ' });
      const rest = line.slice(1).trim();
      const parts = rest.split(/(\s+)/);

      let isFirst = true;
      for (const part of parts) {
        if (/^\s+$/.test(part)) {
          tokens.push({ type: 'text', value: part });
        } else if (part.startsWith('--')) {
          tokens.push({ type: 'flag', value: part });
        } else if (part.startsWith('-') && part.length === 2) {
          tokens.push({ type: 'flag', value: part });
        } else if (/^\d+$/.test(part)) {
          tokens.push({ type: 'value', value: part });
        } else if (isFirst) {
          tokens.push({ type: 'command', value: part });
          isFirst = false;
        } else {
          tokens.push({ type: 'text', value: part });
        }
      }
    } else if (line.startsWith('#')) {
      tokens.push({ type: 'comment', value: line });
    } else {
      tokens.push({ type: 'text', value: line });
    }
    tokens.push({ type: 'text', value: '\n' });
  }

  // Remove trailing newline
  if (tokens.length > 0 && tokens[tokens.length - 1].value === '\n') {
    tokens.pop();
  }

  return tokens;
}

function getTokenClassName(type: Token['type']): string {
  switch (type) {
    case 'prompt':
      return 'text-gray-400';
    case 'command':
      return 'text-cyan-400 font-semibold';
    case 'flag':
      return 'text-yellow-400';
    case 'value':
      return 'text-green-400';
    case 'comment':
      return 'text-gray-500 italic';
    default:
      return 'text-gray-100';
  }
}

export function CodeBlock({
  code,
  language = 'shell',
  showCopyButton = true
}: CodeBlockProps) {
  const { copy, copied } = useCopyToClipboard();

  const handleCopy = () => {
    // Copy code without prompt symbols
    const cleanCode = code
      .split('\n')
      .map(line => line.startsWith('$') ? line.slice(1).trim() : line)
      .join('\n');
    copy(cleanCode);
  };

  const tokens = language === 'shell' ? tokenizeShell(code) : [];

  return (
    <div
      className="relative bg-gray-900 rounded-lg overflow-hidden"
      data-testid="code-block"
    >
      {showCopyButton && (
        <button
          onClick={handleCopy}
          className="absolute top-3 right-3 px-3 py-1.5 text-sm rounded-md bg-gray-700 hover:bg-gray-600 text-gray-300 hover:text-white transition-colors flex items-center gap-2"
          aria-label={copied ? 'Copied!' : 'Copy code'}
          data-testid="copy-button"
        >
          {copied ? (
            <>
              <svg className="w-4 h-4 text-green-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" data-testid="copy-success-icon">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
              <span data-testid="copy-feedback">Copied!</span>
            </>
          ) : (
            <>
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
              </svg>
              <span>Copy</span>
            </>
          )}
        </button>
      )}
      <pre className="p-6 pr-24 overflow-x-auto" data-testid="code-pre">
        <code className="text-sm font-mono" data-testid="code-content" data-language={language}>
          {language === 'shell' && tokens.length > 0 ? (
            tokens.map((token, index) => (
              <span
                key={index}
                className={getTokenClassName(token.type)}
                data-token-type={token.type}
              >
                {token.value}
              </span>
            ))
          ) : (
            <span className="text-gray-100">{code}</span>
          )}
        </code>
      </pre>
    </div>
  );
}
