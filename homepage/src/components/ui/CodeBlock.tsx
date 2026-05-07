'use client';

import React, { useState, useCallback } from 'react';
import { cn } from '@/lib/utils';

interface CodeBlockProps {
  code: string;
  language: string;
  filename?: string;
  className?: string;
}

export default function CodeBlock({
  code,
  language,
  filename,
  className,
}: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback: create a temporary textarea for copying
      const textarea = document.createElement('textarea');
      textarea.value = code;
      textarea.style.position = 'fixed';
      textarea.style.left = '-9999px';
      document.body.appendChild(textarea);
      textarea.select();
      try {
        document.execCommand('copy');
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      } finally {
        document.body.removeChild(textarea);
      }
    }
  }, [code]);

  // Simple syntax highlighting for shell/bash
  const highlightCode = (text: string) => {
    if (language !== 'bash' && language !== 'sh' && language !== 'shell') {
      return text;
    }

    return text.split('\n').map((line, i) => {
      // Highlight comments
      if (line.trim().startsWith('#')) {
        return (
          <span key={i} className="text-slate-400">
            {line}
            {'\n'}
          </span>
        );
      }

      // Highlight prompt lines (lines starting with $)
      if (line.trim().startsWith('$')) {
        const [prompt, ...rest] = line.split('$');
        const command = rest.join('$');
        return (
          <span key={i}>
            <span className="text-brand-400">$</span>
            <span>{command}</span>
            {'\n'}
          </span>
        );
      }

      // Highlight keywords
      const keywords = ['git', 'cargo', 'telnet', 'nc', 'cd'];
      const tokens = line.split(/(\s+)/);
      const highlighted = tokens.map((token, j) => {
        if (keywords.includes(token)) {
          return (
            <span key={j} className="text-purple-400">
              {token}
            </span>
          );
        }
        if (token.startsWith('-')) {
          return (
            <span key={j} className="text-yellow-400">
              {token}
            </span>
          );
        }
        return <span key={j}>{token}</span>;
      });

      return (
        <span key={i}>
          {highlighted}
          {'\n'}
        </span>
      );
    });
  };

  return (
    <div
      className={cn(
        'relative rounded-lg overflow-hidden border',
        'border-[var(--border)] bg-[var(--code-bg)]',
        className
      )}
      data-testid="code-block"
      data-language={language}
    >
      {/* Header bar with language label, filename, and copy button */}
      <div className="flex items-center justify-between px-4 py-2 border-b border-[var(--border)]">
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-[var(--muted)] uppercase tracking-wider">
            {language}
          </span>
          {filename && (
            <span className="text-xs text-[var(--muted-foreground)]">
              {filename}
            </span>
          )}
        </div>
        <button
          type="button"
          onClick={handleCopy}
          className={cn(
            'flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium transition-colors',
            'hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-brand-500',
            copied
              ? 'text-green-400 bg-green-400/10'
              : 'text-[var(--muted)]'
          )}
          aria-label={copied ? 'Copied!' : 'Copy code to clipboard'}
          data-testid="copy-button"
        >
          {copied ? (
            <>
              <svg
                className="w-3.5 h-3.5"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M5 13l4 4L19 7"
                />
              </svg>
              <span>Copied!</span>
            </>
          ) : (
            <>
              <svg
                className="w-3.5 h-3.5"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"
                />
              </svg>
              <span>Copy</span>
            </>
          )}
        </button>
      </div>

      {/* Code content */}
      <pre
        className="p-4 text-sm leading-relaxed overflow-x-auto text-[var(--code-fg)]"
        data-testid="code-content"
      >
        <code className={`language-${language}`}>{highlightCode(code)}</code>
      </pre>
    </div>
  );
}
