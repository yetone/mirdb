import React, { useState, useCallback } from 'react';
import './QuickStart.css';

/**
 * Quick Start Section Component
 * Owner: Scenario 4 - Quick Start Section
 *
 * Displays installation and first-use commands:
 * - Cargo add command
 * - Basic usage/run commands
 * - Copy buttons for each command
 */

interface CommandBlockProps {
  label: string;
  command: string;
  testId: string;
}

const CommandBlock: React.FC<CommandBlockProps> = ({ label, command, testId }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = useCallback(async () => {
    // Always show visual feedback
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);

    // Try clipboard API first, then fallback to execCommand
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(command);
      } else {
        // Fallback for browsers/contexts without clipboard API
        const textArea = document.createElement('textarea');
        textArea.value = command;
        textArea.style.position = 'fixed';
        textArea.style.left = '-999999px';
        document.body.appendChild(textArea);
        textArea.select();
        document.execCommand('copy');
        document.body.removeChild(textArea);
      }
    } catch (err) {
      // Visual feedback already shown, just log the error
      console.warn('Clipboard copy failed, text may not have been copied:', err);
    }
  }, [command]);

  return (
    <div className="quickstart__command-block" data-testid={testId}>
      <span className="quickstart__command-label">{label}</span>
      <div className="quickstart__command-container">
        <code className="quickstart__command-code" data-testid={`${testId}-code`}>
          {command}
        </code>
        <button
          className={`quickstart__copy-button ${copied ? 'quickstart__copy-button--copied' : ''}`}
          onClick={handleCopy}
          aria-label={copied ? 'Copied!' : `Copy ${label} command`}
          data-testid={`${testId}-copy-button`}
        >
          {copied ? (
            <svg
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
          ) : (
            <svg
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
          <span className="quickstart__copy-text">{copied ? 'Copied!' : 'Copy'}</span>
        </button>
      </div>
    </div>
  );
};

export const QuickStart: React.FC = () => {
  return (
    <section
      id="quickstart"
      className="quickstart"
      aria-labelledby="quickstart-heading"
      data-testid="quickstart-section"
    >
      <div className="quickstart__container container">
        <h2 id="quickstart-heading" className="quickstart__heading" data-testid="quickstart-heading">
          Quick Start
        </h2>
        <p className="quickstart__description">
          Get up and running with MirDB in just a few commands
        </p>

        <div className="quickstart__steps">
          <div className="quickstart__step" data-testid="quickstart-step-install">
            <h3 className="quickstart__step-title">
              <span className="quickstart__step-number">1</span>
              Install MirDB
            </h3>
            <CommandBlock
              label="Add to Cargo.toml"
              command="cargo add mirdb"
              testId="quickstart-install-command"
            />
          </div>

          <div className="quickstart__step" data-testid="quickstart-step-usage">
            <h3 className="quickstart__step-title">
              <span className="quickstart__step-number">2</span>
              Basic Usage
            </h3>
            <CommandBlock
              label="Initialize and run"
              command="use mirdb::DB;

let db = DB::open(&quot;my_database&quot;)?;
db.put(b&quot;key&quot;, b&quot;value&quot;)?;
let value = db.get(b&quot;key&quot;)?;"
              testId="quickstart-usage-command"
            />
          </div>

          <div className="quickstart__step" data-testid="quickstart-step-run">
            <h3 className="quickstart__step-title">
              <span className="quickstart__step-number">3</span>
              Run Your Project
            </h3>
            <CommandBlock
              label="Build and run"
              command="cargo run"
              testId="quickstart-run-command"
            />
          </div>
        </div>
      </div>
    </section>
  );
};

export default QuickStart;
