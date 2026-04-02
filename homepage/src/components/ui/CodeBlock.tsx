/**
 * Code block component with syntax highlighting and copy functionality.
 * Owner: Scenario 4 - Quick Start Section with Code Examples
 */
import React, { useEffect, useRef } from 'react';
import Prism from 'prismjs';
import 'prismjs/components/prism-bash';
import 'prismjs/components/prism-python';
import { useCopyToClipboard } from '@/hooks/useCopyToClipboard';
import styles from './CodeBlock.module.css';

export interface CodeBlockProps {
  code: string;
  language: string;
  title?: string;
}

export function CodeBlock({ code, language, title }: CodeBlockProps) {
  const codeRef = useRef<HTMLElement>(null);
  const { copy, copied } = useCopyToClipboard();

  useEffect(() => {
    if (codeRef.current) {
      Prism.highlightElement(codeRef.current);
    }
  }, [code, language]);

  const handleCopy = async () => {
    await copy(code);
  };

  return (
    <div className={styles.codeBlock} data-testid="code-block">
      {title && (
        <div className={styles.header}>
          <span className={styles.title}>{title}</span>
          <button
            type="button"
            className={styles.copyButton}
            onClick={handleCopy}
            aria-label={copied ? 'Copied!' : 'Copy code'}
            data-testid="copy-button"
          >
            {copied ? (
              <span className={styles.copiedIcon}>&#10003;</span>
            ) : (
              <svg
                className={styles.copyIcon}
                width="16"
                height="16"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                aria-hidden="true"
              >
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
              </svg>
            )}
            <span className={styles.copyText}>{copied ? 'Copied!' : 'Copy'}</span>
          </button>
        </div>
      )}
      <pre className={styles.pre}>
        <code
          ref={codeRef}
          className={`language-${language} ${styles.code}`}
          data-testid="code-content"
        >
          {code}
        </code>
      </pre>
      {copied && (
        <div className={styles.copiedFeedback} role="status" aria-live="polite">
          Copied to clipboard!
        </div>
      )}
    </div>
  );
}
