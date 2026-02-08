/**
 * Installation Instructions Component
 * Owner: Scenario 3 - Installation Instructions
 *
 * Displays installation code snippets with copy-to-clipboard functionality.
 */

import { useState } from 'react';
import { copyToClipboard } from '../../utils/clipboard';
import { INSTALLATION_STEPS } from '../../utils/constants';
import './Installation.css';

interface CodeBlockWithCopyProps {
  code: string;
  onCopy: () => void;
  copied: boolean;
}

function CodeBlockWithCopy({ code, onCopy, copied }: CodeBlockWithCopyProps) {
  return (
    <div className="code-block">
      <pre>
        <code>{code}</code>
      </pre>
      <button
        className="copy-button"
        onClick={onCopy}
        aria-label={copied ? 'Copied to clipboard' : 'Copy to clipboard'}
        type="button"
      >
        {copied ? (
          <>
            <CheckIcon />
            <span>Copied!</span>
          </>
        ) : (
          <>
            <CopyIcon />
            <span>Copy</span>
          </>
        )}
      </button>
    </div>
  );
}

function CopyIcon() {
  return (
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
      <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
      <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
    </svg>
  );
}

function CheckIcon() {
  return (
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
      <polyline points="20 6 9 17 4 12" />
    </svg>
  );
}

export function Installation() {
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);

  const handleCopy = async (code: string, index: number) => {
    const success = await copyToClipboard(code);
    if (success) {
      setCopiedIndex(index);
      setTimeout(() => setCopiedIndex(null), 2000);
    }
  };

  return (
    <section id="installation" className="installation" aria-labelledby="installation-title">
      <div className="installation-container">
        <h2 id="installation-title" className="installation-title">
          Installation
        </h2>
        <p className="installation-description">
          Get started with MirDB in just a few steps.
        </p>

        <div className="installation-steps">
          {INSTALLATION_STEPS.map((step, index) => (
            <div key={step.id} className="installation-step">
              <div className="step-header">
                <span className="step-number">{index + 1}</span>
                <h3 className="step-title">{step.label}</h3>
              </div>
              <CodeBlockWithCopy
                code={step.command}
                onCopy={() => handleCopy(step.command, index)}
                copied={copiedIndex === index}
              />
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
