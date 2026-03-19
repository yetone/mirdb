/**
 * CodeBlock component with syntax highlighting and copy button.
 * Owner: Scenario 5 - Quick Start Section with Code Examples
 *
 * Props:
 * - code: string
 * - language: string (bash, rust, etc.)
 * - showLineNumbers?: boolean
 *
 * Features:
 * - Syntax highlighting (CSS-based for bash commands)
 * - Copy to clipboard button
 * - Language indicator label
 * - Dark/light theme support
 *
 * Requirements: REQ-7
 */

'use client';

import React from 'react';
import { Copy, Check } from 'lucide-react';
import { cn } from '@/lib/utils';
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard';

export interface CodeBlockProps {
  code: string;
  language?: string;
  showLineNumbers?: boolean;
  className?: string;
}

/**
 * Apply basic syntax highlighting for bash commands
 */
function highlightBash(code: string): React.ReactNode[] {
  const parts: React.ReactNode[] = [];
  let remaining = code;
  let key = 0;

  // Common bash commands to highlight
  const commands = ['git', 'cd', 'cargo', 'npm', 'yarn', 'pnpm', 'telnet', 'echo', 'set', 'get'];
  const flags = /--?\w+(?:-\w+)*/g;
  const strings = /"[^"]*"|'[^']*'/g;
  const urls = /https?:\/\/[^\s]+/g;

  // Simple tokenization approach
  const tokens: { type: string; value: string; index: number }[] = [];

  // Find commands at the start or after newlines
  const lines = code.split('\n');
  let currentIndex = 0;

  lines.forEach((line, lineIndex) => {
    const trimmedLine = line.trim();
    const leadingSpaces = line.length - line.trimStart().length;

    // Check if line starts with a command
    for (const cmd of commands) {
      if (trimmedLine.startsWith(cmd + ' ') || trimmedLine === cmd) {
        tokens.push({
          type: 'command',
          value: cmd,
          index: currentIndex + leadingSpaces,
        });
        break;
      }
    }

    // Find flags
    let match;
    const flagRegex = /--?\w+(?:-\w+)*/g;
    while ((match = flagRegex.exec(line)) !== null) {
      tokens.push({
        type: 'flag',
        value: match[0],
        index: currentIndex + match.index,
      });
    }

    // Find URLs
    const urlRegex = /https?:\/\/[^\s]+/g;
    while ((match = urlRegex.exec(line)) !== null) {
      tokens.push({
        type: 'url',
        value: match[0],
        index: currentIndex + match.index,
      });
    }

    // Find strings
    const stringRegex = /"[^"]*"|'[^']*'/g;
    while ((match = stringRegex.exec(line)) !== null) {
      tokens.push({
        type: 'string',
        value: match[0],
        index: currentIndex + match.index,
      });
    }

    currentIndex += line.length + (lineIndex < lines.length - 1 ? 1 : 0);
  });

  // Sort tokens by index
  tokens.sort((a, b) => a.index - b.index);

  // Build highlighted output
  let lastIndex = 0;
  for (const token of tokens) {
    // Add text before this token
    if (token.index > lastIndex) {
      parts.push(
        <span key={key++}>{code.slice(lastIndex, token.index)}</span>
      );
    }

    // Skip if this token overlaps with previous
    if (token.index < lastIndex) continue;

    // Add highlighted token
    const className = {
      command: 'text-green-400',
      flag: 'text-yellow-400',
      url: 'text-blue-400',
      string: 'text-orange-400',
    }[token.type];

    parts.push(
      <span key={key++} className={className}>
        {token.value}
      </span>
    );

    lastIndex = token.index + token.value.length;
  }

  // Add remaining text
  if (lastIndex < code.length) {
    parts.push(<span key={key++}>{code.slice(lastIndex)}</span>);
  }

  return parts.length > 0 ? parts : [<span key={0}>{code}</span>];
}

export function CodeBlock({
  code,
  language = 'bash',
  showLineNumbers = false,
  className,
}: CodeBlockProps) {
  const { copy, copied } = useCopyToClipboard();

  const handleCopy = async () => {
    await copy(code);
  };

  const lines = code.split('\n');
  const isBash = language === 'bash' || language === 'shell' || language === 'sh';

  return (
    <div
      className={cn(
        'relative group rounded-lg overflow-hidden',
        'bg-slate-900 dark:bg-slate-950',
        className
      )}
      data-testid="code-block"
    >
      {/* Language label */}
      <div className="flex items-center justify-between px-4 py-2 bg-slate-800 dark:bg-slate-900 border-b border-slate-700">
        <span className="text-xs font-mono text-slate-400 uppercase" data-testid="code-language">
          {language}
        </span>
        {/* Copy button */}
        <button
          onClick={handleCopy}
          className={cn(
            'flex items-center gap-1.5 px-2 py-1 rounded text-xs font-medium transition-all',
            'hover:bg-slate-700 focus:outline-none focus:ring-2 focus:ring-accent-500',
            copied
              ? 'text-green-400 bg-green-400/10'
              : 'text-slate-400 hover:text-white'
          )}
          aria-label={copied ? 'Copied!' : 'Copy code to clipboard'}
          data-testid="copy-button"
        >
          {copied ? (
            <>
              <Check className="w-4 h-4" aria-hidden="true" />
              <span>Copied!</span>
            </>
          ) : (
            <>
              <Copy className="w-4 h-4" aria-hidden="true" />
              <span>Copy</span>
            </>
          )}
        </button>
      </div>

      {/* Code content */}
      <pre
        className={cn(
          'p-4 overflow-x-auto text-sm font-mono',
          'text-slate-200'
        )}
        data-testid="code-content"
      >
        <code className={`language-${language}`}>
          {showLineNumbers ? (
            lines.map((line, index) => (
              <div key={index} className="flex">
                <span className="select-none text-slate-600 w-8 text-right mr-4">
                  {index + 1}
                </span>
                <span className="flex-1">
                  {isBash ? highlightBash(line) : line}
                </span>
              </div>
            ))
          ) : (
            isBash ? highlightBash(code) : code
          )}
        </code>
      </pre>
    </div>
  );
}
