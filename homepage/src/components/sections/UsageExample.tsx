/**
 * Usage Example Section Component
 * Owner: Scenario 3 - Usage Example Section
 *
 * Displays a terminal-style demonstration showing:
 * - SET command example
 * - GET command example
 * - Animated GIF or terminal mockup
 */

import React from 'react';

interface TerminalLine {
  type: 'prompt' | 'command' | 'output';
  content: string;
}

const terminalLines: TerminalLine[] = [
  { type: 'prompt', content: '$ ' },
  { type: 'command', content: 'telnet 127.0.0.1 11211' },
  { type: 'output', content: 'Trying 127.0.0.1...' },
  { type: 'output', content: 'Connected to localhost.' },
  { type: 'prompt', content: '> ' },
  { type: 'command', content: 'set mykey 0 0 5' },
  { type: 'output', content: 'hello' },
  { type: 'output', content: 'STORED' },
  { type: 'prompt', content: '> ' },
  { type: 'command', content: 'get mykey' },
  { type: 'output', content: 'VALUE mykey 0 5' },
  { type: 'output', content: 'hello' },
  { type: 'output', content: 'END' },
];

const sectionStyles: React.CSSProperties = {
  padding: 'var(--spacing-3xl) 0',
  backgroundColor: 'var(--bg-secondary)',
};

const containerStyles: React.CSSProperties = {
  maxWidth: 'var(--container-max-width)',
  margin: '0 auto',
  padding: '0 var(--container-padding)',
};

const headerStyles: React.CSSProperties = {
  textAlign: 'center',
  marginBottom: 'var(--spacing-2xl)',
};

const titleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-3xl)',
  fontWeight: 700,
  color: 'var(--text-primary)',
  marginBottom: 'var(--spacing-md)',
};

const subtitleStyles: React.CSSProperties = {
  fontSize: 'var(--font-size-lg)',
  color: 'var(--text-secondary)',
  maxWidth: '600px',
  margin: '0 auto',
};

const terminalStyles: React.CSSProperties = {
  backgroundColor: '#1e1e1e',
  borderRadius: 'var(--radius-lg)',
  overflow: 'hidden',
  boxShadow: 'var(--shadow-lg)',
  fontFamily: "'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, monospace",
  marginBottom: 'var(--spacing-2xl)',
};

const terminalHeaderStyles: React.CSSProperties = {
  background: 'linear-gradient(180deg, #3c3c3c 0%, #2c2c2c 100%)',
  padding: 'var(--spacing-sm) var(--spacing-md)',
  display: 'flex',
  alignItems: 'center',
  gap: 'var(--spacing-md)',
};

const terminalButtonsStyles: React.CSSProperties = {
  display: 'flex',
  gap: 'var(--spacing-sm)',
};

const terminalButtonBase: React.CSSProperties = {
  width: '12px',
  height: '12px',
  borderRadius: '50%',
};

const terminalTitleStyles: React.CSSProperties = {
  color: '#999',
  fontSize: 'var(--font-size-sm)',
  flex: 1,
  textAlign: 'center',
};

const terminalBodyStyles: React.CSSProperties = {
  padding: 'var(--spacing-lg)',
  minHeight: '300px',
};

const terminalLineStyles: React.CSSProperties = {
  lineHeight: 1.8,
  fontSize: 'var(--font-size-sm)',
};

const promptTextStyles: React.CSSProperties = {
  color: '#58a6ff',
};

const commandTextStyles: React.CSSProperties = {
  color: '#39ff14',
};

const outputTextStyles: React.CSSProperties = {
  color: '#ccc',
};

const gifContainerStyles: React.CSSProperties = {
  textAlign: 'center',
};

const gifStyles: React.CSSProperties = {
  maxWidth: '100%',
  borderRadius: 'var(--radius-lg)',
  boxShadow: 'var(--shadow-md)',
};

export const UsageExample: React.FC = () => {
  return (
    <section id="usage" style={sectionStyles} data-testid="usage-example-section" aria-labelledby="usage-title">
      <div style={containerStyles}>
        <header style={headerStyles}>
          <h2 id="usage-title" style={titleStyles}>
            Usage Example
          </h2>
          <p style={subtitleStyles}>
            MirDB uses the Memcached protocol. Here&apos;s how to interact with it:
          </p>
        </header>

        <div
          style={terminalStyles}
          data-testid="terminal-display"
          role="img"
          aria-label="Terminal demonstration of SET and GET commands"
        >
          <div style={terminalHeaderStyles}>
            <div style={terminalButtonsStyles}>
              <span style={{ ...terminalButtonBase, backgroundColor: '#ff5f56' }} className="terminal-button close"></span>
              <span style={{ ...terminalButtonBase, backgroundColor: '#ffbd2e' }} className="terminal-button minimize"></span>
              <span style={{ ...terminalButtonBase, backgroundColor: '#27ca40' }} className="terminal-button maximize"></span>
            </div>
            <span style={terminalTitleStyles}>Terminal - MirDB</span>
          </div>
          <div style={terminalBodyStyles}>
            {terminalLines.map((line, index) => (
              <div key={index} style={terminalLineStyles}>
                {line.type === 'prompt' && (
                  <span style={promptTextStyles}>{line.content}</span>
                )}
                {line.type === 'command' && (
                  <span
                    style={commandTextStyles}
                    data-testid={
                      line.content.includes('set ')
                        ? 'set-command'
                        : line.content.includes('get ')
                        ? 'get-command'
                        : undefined
                    }
                  >
                    {line.content}
                  </span>
                )}
                {line.type === 'output' && <span style={outputTextStyles}>{line.content}</span>}
              </div>
            ))}
          </div>
        </div>

        <div style={gifContainerStyles} data-testid="usage-gif-container">
          <img
            src="/assets/images/usage.gif"
            alt="MirDB usage demonstration showing SET and GET commands"
            style={gifStyles}
            data-testid="usage-gif"
            loading="lazy"
          />
        </div>
      </div>
    </section>
  );
};

export default UsageExample;
