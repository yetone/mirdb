/**
 * Usage Example section component for MirDB homepage.
 * Owner: Scenario 5 - Usage Example Display
 *
 * Requirements:
 * - Display memcached commands in action (REQ-5)
 * - Code snippet or animated demonstration
 * - Syntax highlighting for commands
 *
 * Expected exports:
 * - UsageExample: React.FC
 */

import React from 'react'

interface CodeLineProps {
  prompt?: string
  command?: string
  args?: string
  response?: string
  comment?: string
}

const CodeLine: React.FC<CodeLineProps> = ({ prompt, command, args, response, comment }) => {
  if (comment) {
    return (
      <div className="text-slate-500" data-testid="code-comment">
        <span className="syntax-comment">{comment}</span>
      </div>
    )
  }

  if (response) {
    return (
      <div className="text-emerald-400" data-testid="code-response">
        <span className="syntax-response">{response}</span>
      </div>
    )
  }

  return (
    <div>
      {prompt && <span className="text-slate-400 syntax-prompt">{prompt}</span>}
      {command && <span className="text-cyan-400 font-semibold syntax-command">{command}</span>}
      {args && <span className="text-amber-300 syntax-args">{args}</span>}
    </div>
  )
}

export const UsageExample: React.FC = () => {
  const codeLines: CodeLineProps[] = [
    { comment: '# Connect to MirDB using any memcached client' },
    { comment: '# Default port: 12333' },
    { prompt: '$ ', command: 'telnet ', args: 'localhost 12333' },
    { response: 'Connected to localhost.' },
    { comment: '' },
    { comment: '# Store a value with SET command' },
    { comment: '# Format: set <key> <flags> <ttl> <bytes>' },
    { prompt: '> ', command: 'set ', args: 'mykey 0 3600 5' },
    { prompt: '> ', command: '', args: 'hello' },
    { response: 'STORED' },
    { comment: '' },
    { comment: '# Retrieve the value with GET command' },
    { prompt: '> ', command: 'get ', args: 'mykey' },
    { response: 'VALUE mykey 0 5' },
    { response: 'hello' },
    { response: 'END' },
    { comment: '' },
    { comment: '# Delete the key' },
    { prompt: '> ', command: 'delete ', args: 'mykey' },
    { response: 'DELETED' },
  ]

  return (
    <section
      id="usage-example"
      className="py-20 px-4 bg-slate-800/50"
      aria-labelledby="usage-example-title"
    >
      <div className="max-w-4xl mx-auto">
        <h2
          id="usage-example-title"
          className="text-3xl font-bold mb-4 text-center"
        >
          See It in Action
        </h2>
        <p className="text-slate-300 text-center mb-8 max-w-2xl mx-auto">
          MirDB speaks the memcached protocol, so you can use any memcached client
          to interact with it. Here&apos;s a quick example:
        </p>

        <div className="relative">
          <div className="absolute -inset-1 bg-gradient-to-r from-cyan-500/20 to-emerald-500/20 rounded-lg blur-sm" />
          <pre
            className="relative bg-slate-900 rounded-lg p-6 overflow-x-auto font-mono text-sm leading-relaxed syntax-highlighted"
            role="region"
            aria-label="Memcached usage example code"
            data-testid="usage-example-code"
          >
            <code className="block">
              {codeLines.map((line, index) => (
                <CodeLine key={index} {...line} />
              ))}
            </code>
          </pre>
        </div>

        <div className="mt-6 text-center">
          <p className="text-slate-400 text-sm">
            MirDB persists your data to disk using LSM-tree architecture,
            so your data survives server restarts.
          </p>
        </div>
      </div>
    </section>
  )
}

export default UsageExample
