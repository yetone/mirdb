/**
 * Getting Started Section Component
 * Owner: Scenario 4 - Getting Started Section
 *
 * Displays quick start commands with:
 * - Clone repository command
 * - Build project command
 * - Run server command
 * - Copy-to-clipboard functionality for each command
 */

import React, { useState, useCallback } from 'react';
import { CodeBlock } from '../ui/CodeBlock';
import { Toast } from '../ui/Toast';
import { useCopyToClipboard, CopyState } from '../../hooks/useCopyToClipboard';
import { GITHUB_URL } from '../../utils/constants';

const sectionStyles: React.CSSProperties = {
  padding: 'var(--spacing-3xl) var(--container-padding)',
  backgroundColor: 'var(--bg-primary)',
};

const containerStyles: React.CSSProperties = {
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
};

const titleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-3xl)',
  fontWeight: 700,
  marginBottom: 'var(--spacing-md)',
  color: 'var(--text-primary)',
  textAlign: 'center' as const,
};

const descriptionStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-lg)',
  color: 'var(--text-secondary)',
  marginBottom: 'var(--spacing-2xl)',
  textAlign: 'center' as const,
  maxWidth: '600px',
  margin: '0 auto var(--spacing-2xl)',
};

const codeBlocksContainerStyles: React.CSSProperties = {
  maxWidth: '700px',
  margin: '0 auto',
};

interface CodeCommand {
  id: string;
  label: string;
  code: string;
  language: string;
}

const commands: CodeCommand[] = [
  {
    id: 'clone',
    label: 'Clone Repository',
    code: `git clone ${GITHUB_URL}.git`,
    language: 'bash',
  },
  {
    id: 'build',
    label: 'Build Project',
    code: 'cargo build --release',
    language: 'bash',
  },
  {
    id: 'run',
    label: 'Run Server',
    code: 'cargo run --release',
    language: 'bash',
  },
];

export function GettingStarted() {
  const { copy, state: globalCopyState } = useCopyToClipboard();
  const [copyStates, setCopyStates] = useState<Record<string, CopyState>>({});
  const [toastVisible, setToastVisible] = useState(false);
  const [toastMessage, setToastMessage] = useState('');
  const [toastType, setToastType] = useState<'success' | 'error'>('success');

  const handleCopy = useCallback(async (id: string, code: string) => {
    setCopyStates(prev => ({ ...prev, [id]: 'idle' }));

    try {
      await navigator.clipboard.writeText(code);
      setCopyStates(prev => ({ ...prev, [id]: 'success' }));
      setToastMessage('Copied to clipboard!');
      setToastType('success');
      setToastVisible(true);

      setTimeout(() => {
        setCopyStates(prev => ({ ...prev, [id]: 'idle' }));
      }, 2000);
    } catch (error) {
      setCopyStates(prev => ({ ...prev, [id]: 'error' }));
      setToastMessage('Failed to copy');
      setToastType('error');
      setToastVisible(true);

      setTimeout(() => {
        setCopyStates(prev => ({ ...prev, [id]: 'idle' }));
      }, 2000);
    }
  }, []);

  const handleToastClose = useCallback(() => {
    setToastVisible(false);
  }, []);

  return (
    <section
      id="getting-started"
      data-testid="getting-started-section"
      style={sectionStyles}
      aria-labelledby="getting-started-title"
    >
      <div style={containerStyles}>
        <h2 id="getting-started-title" style={titleStyles}>
          Getting Started
        </h2>
        <p style={descriptionStyles}>
          Get up and running with MirDB in just a few steps.
        </p>
        <div style={codeBlocksContainerStyles}>
          {commands.map((command) => (
            <CodeBlock
              key={command.id}
              code={command.code}
              language={command.language}
              label={command.label}
              onCopy={(code) => handleCopy(command.id, code)}
              copyState={copyStates[command.id] || 'idle'}
            />
          ))}
        </div>
      </div>
      <Toast
        message={toastMessage}
        type={toastType}
        visible={toastVisible}
        onClose={handleToastClose}
        duration={2000}
      />
    </section>
  );
}
