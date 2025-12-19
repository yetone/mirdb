import React, { useState, useCallback } from 'react';

export interface CodeBlockProps {
  code: string;
  language?: string;
  title?: string;
  showLineNumbers?: boolean;
}

export const CodeBlock: React.FC<CodeBlockProps> = ({
  code,
  language = 'bash',
  title,
  showLineNumbers = false,
}) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      // Fallback for browsers that don't support clipboard API
      const textArea = document.createElement('textarea');
      textArea.value = code;
      textArea.style.position = 'fixed';
      textArea.style.left = '-999999px';
      textArea.style.top = '-999999px';
      document.body.appendChild(textArea);
      textArea.focus();
      textArea.select();
      try {
        document.execCommand('copy');
        setCopied(true);
        setTimeout(() => setCopied(false), 2000);
      } catch {
        console.error('Failed to copy code');
      }
      textArea.remove();
    }
  }, [code]);

  const renderCodeLines = () => {
    const lines = code.split('\n');
    if (!showLineNumbers) {
      return <code className={`language-${language}`}>{code}</code>;
    }
    return (
      <code className={`language-${language}`}>
        {lines.map((line, index) => (
          <span key={index} className="code-line">
            <span className="line-number">{index + 1}</span>
            <span className="line-content">{line}</span>
            {index < lines.length - 1 && '\n'}
          </span>
        ))}
      </code>
    );
  };

  return (
    <div className="code-block" data-testid="code-block">
      {title && (
        <div className="code-block-header" data-testid="code-block-header">
          <span className="code-block-title">{title}</span>
          <span className="code-block-language">{language}</span>
        </div>
      )}
      <div className="code-block-content">
        <pre className={`code-pre language-${language}`} data-testid="code-pre">
          {renderCodeLines()}
        </pre>
        <button
          className="copy-button"
          onClick={handleCopy}
          aria-label={copied ? 'Copied!' : 'Copy code'}
          data-testid="copy-button"
          type="button"
        >
          {copied ? (
            <span className="copy-icon copied" data-testid="copied-icon">
              ✓
            </span>
          ) : (
            <span className="copy-icon" data-testid="copy-icon">
              <svg
                xmlns="http://www.w3.org/2000/svg"
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
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
              </svg>
            </span>
          )}
        </button>
      </div>
    </div>
  );
};

export default CodeBlock;
