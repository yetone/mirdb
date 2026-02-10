/**
 * Code Block Component
 * Owner: Scenario 4 - Getting Started Section
 *
 * Displays a styled code block with:
 * - Syntax highlighting
 * - Copy-to-clipboard button
 * - Language indicator
 */

import React from 'react';

export interface CodeBlockProps {
  code: string;
  language?: string;
  label?: string;
  onCopy: (code: string) => void;
  copyState?: 'idle' | 'success' | 'error';
}

const containerStyles: React.CSSProperties = {
  position: 'relative',
  marginBottom: 'var(--spacing-md)',
};

const labelStyles: React.CSSProperties = {
  display: 'flex',
  justifyContent: 'space-between',
  alignItems: 'center',
  backgroundColor: 'var(--bg-tertiary)',
  padding: '8px 16px',
  borderTopLeftRadius: 'var(--radius-md)',
  borderTopRightRadius: 'var(--radius-md)',
  borderBottom: '1px solid var(--border-color)',
  fontSize: 'var(--font-size-sm)',
  color: 'var(--text-secondary)',
};

const codeContainerStyles: React.CSSProperties = {
  backgroundColor: 'var(--bg-secondary)',
  borderRadius: '0 0 var(--radius-md) var(--radius-md)',
  overflow: 'hidden',
};

const preStyles: React.CSSProperties = {
  margin: 0,
  padding: '16px',
  overflow: 'auto',
  fontSize: 'var(--font-size-sm)',
  fontFamily: 'ui-monospace, SFMono-Regular, "SF Mono", Menlo, Monaco, Consolas, monospace',
  lineHeight: 1.6,
  color: 'var(--text-primary)',
};

const codeStyles: React.CSSProperties = {
  fontFamily: 'inherit',
};

const copyButtonStyles: React.CSSProperties = {
  display: 'flex',
  alignItems: 'center',
  gap: '4px',
  padding: '4px 8px',
  border: '1px solid var(--border-color)',
  borderRadius: 'var(--radius-sm)',
  backgroundColor: 'transparent',
  color: 'var(--text-secondary)',
  cursor: 'pointer',
  fontSize: 'var(--font-size-sm)',
  transition: 'all 0.2s ease',
};

export function CodeBlock({
  code,
  language = 'bash',
  label,
  onCopy,
  copyState = 'idle',
}: CodeBlockProps) {
  const handleCopy = () => {
    onCopy(code);
  };

  const getCopyButtonText = () => {
    switch (copyState) {
      case 'success':
        return 'Copied!';
      case 'error':
        return 'Failed';
      default:
        return 'Copy';
    }
  };

  const getCopyButtonStyle = (): React.CSSProperties => {
    if (copyState === 'success') {
      return { ...copyButtonStyles, backgroundColor: '#10b981', color: '#ffffff', borderColor: '#10b981' };
    }
    if (copyState === 'error') {
      return { ...copyButtonStyles, backgroundColor: '#ef4444', color: '#ffffff', borderColor: '#ef4444' };
    }
    return copyButtonStyles;
  };

  return (
    <div style={containerStyles} data-testid="code-block">
      <div style={labelStyles}>
        <span data-testid="code-block-label">{label || language}</span>
        <button
          type="button"
          onClick={handleCopy}
          style={getCopyButtonStyle()}
          aria-label={`Copy ${label || 'code'} to clipboard`}
          data-testid="copy-button"
        >
          {copyState === 'idle' && (
            <svg
              width="14"
              height="14"
              viewBox="0 0 14 14"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
            >
              <rect
                x="4.5"
                y="4.5"
                width="8"
                height="8"
                rx="1"
                stroke="currentColor"
                strokeWidth="1.5"
              />
              <path
                d="M9.5 4.5V2.5C9.5 1.94772 9.05228 1.5 8.5 1.5H2.5C1.94772 1.5 1.5 1.94772 1.5 2.5V8.5C1.5 9.05228 1.94772 9.5 2.5 9.5H4.5"
                stroke="currentColor"
                strokeWidth="1.5"
              />
            </svg>
          )}
          {copyState === 'success' && (
            <svg
              width="14"
              height="14"
              viewBox="0 0 14 14"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
            >
              <path
                d="M11.5 4L5.5 10L2.5 7"
                stroke="currentColor"
                strokeWidth="1.5"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          )}
          {getCopyButtonText()}
        </button>
      </div>
      <div style={codeContainerStyles}>
        <pre style={preStyles}>
          <code style={codeStyles} data-testid="code-content">
            {code}
          </code>
        </pre>
      </div>
    </div>
  );
}
