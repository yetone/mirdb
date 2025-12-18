// Configuration section component showing MirDB default configuration
// Based on PRD REQ-6: Show configuration example with default parameters

// TOML configuration content with default values from PRD appendix
const CONFIG_TOML = `addr = "0.0.0.0:12333"

max_level = 7
work_dir = "/tmp/mirdb"

sst_max_size = "100M"
mem_table_max_size = "4M"
mem_table_max_height = 32

imm_mem_table_max_count = 16

block_size = "4K"
block_restart_interval = 16

l0_compaction_trigger = 4

thread_sleep_ms = 500`

interface ConfigToken {
  type: 'key' | 'equals' | 'string' | 'number' | 'comment'
  content: string
}

// Parse and tokenize TOML line for syntax highlighting
function tokenizeTomlLine(line: string): ConfigToken[] {
  const tokens: ConfigToken[] = []

  // Empty line
  if (line.trim() === '') {
    return tokens
  }

  // Comment line
  if (line.trim().startsWith('#')) {
    tokens.push({ type: 'comment', content: line })
    return tokens
  }

  // Key = Value line
  const match = line.match(/^(\s*)(\w+)(\s*=\s*)(.+)$/)
  if (match) {
    const [, indent, key, equals, value] = match

    if (indent) {
      tokens.push({ type: 'key', content: indent })
    }
    tokens.push({ type: 'key', content: key })
    tokens.push({ type: 'equals', content: equals })

    // Determine value type
    if (value.startsWith('"') && value.endsWith('"')) {
      tokens.push({ type: 'string', content: value })
    } else {
      tokens.push({ type: 'number', content: value })
    }

    return tokens
  }

  // Default: treat as comment/text
  tokens.push({ type: 'comment', content: line })
  return tokens
}

export default function ConfigurationSection() {
  const lines = CONFIG_TOML.split('\n')

  return (
    <section className="configuration-section" aria-labelledby="configuration-title">
      <h2 id="configuration-title">Configuration</h2>
      <p className="configuration-description">
        MirDB uses TOML configuration files. Below are the default parameters:
      </p>

      <div className="config-container">
        <div className="config-code-block" data-testid="config-code-block">
          <div className="config-header">
            <span className="config-filename">mirdb.toml</span>
          </div>
          <pre>
            <code>
              {lines.map((line, lineIndex) => {
                const tokens = tokenizeTomlLine(line)
                return (
                  <div key={lineIndex} className="config-line">
                    {tokens.length === 0 ? (
                      <span>{'\u00A0'}</span>
                    ) : (
                      tokens.map((token, tokenIndex) => (
                        <span key={tokenIndex} className={`token-${token.type}`}>
                          {token.content}
                        </span>
                      ))
                    )}
                    {'\n'}
                  </div>
                )
              })}
            </code>
          </pre>
        </div>

        <div className="config-summary">
          <h3>Default Values</h3>
          <table role="table">
            <thead>
              <tr>
                <th>Parameter</th>
                <th>Default</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td>Port</td>
                <td>12333</td>
                <td>Listen port for memcached protocol</td>
              </tr>
              <tr>
                <td>Max LSM levels</td>
                <td>7</td>
                <td>Maximum depth of LSM tree</td>
              </tr>
              <tr>
                <td>Memtable size</td>
                <td>4MB</td>
                <td>Maximum size before flush</td>
              </tr>
              <tr>
                <td>SSTable max size</td>
                <td>100MB</td>
                <td>Maximum SSTable file size</td>
              </tr>
              <tr>
                <td>Block size</td>
                <td>4KB</td>
                <td>Data block size</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <style>{`
        .configuration-section {
          padding: 4rem 2rem;
          background: #f8f9fa;
          color: #1a1a2e;
        }

        #configuration-title {
          font-size: 2rem;
          margin-bottom: 1rem;
          text-align: center;
          color: #1a1a2e;
        }

        .configuration-description {
          text-align: center;
          color: #6c757d;
          margin-bottom: 2rem;
        }

        .config-container {
          max-width: 900px;
          margin: 0 auto;
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 2rem;
          align-items: start;
        }

        @media (max-width: 768px) {
          .config-container {
            grid-template-columns: 1fr;
          }
        }

        .config-code-block {
          background: #0d1117;
          border-radius: 8px;
          overflow: hidden;
        }

        .config-header {
          background: #161b22;
          padding: 0.75rem 1rem;
          border-bottom: 1px solid #30363d;
        }

        .config-filename {
          color: #8b949e;
          font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
          font-size: 13px;
        }

        .config-code-block pre {
          margin: 0;
          padding: 1rem;
          font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
          font-size: 13px;
          line-height: 1.6;
          overflow-x: auto;
        }

        .config-code-block code {
          display: block;
          color: #c9d1d9;
        }

        .config-line {
          display: block;
          min-height: 1.4em;
        }

        /* TOML syntax highlighting */
        .token-key {
          color: #79c0ff;
        }

        .token-equals {
          color: #c9d1d9;
        }

        .token-string {
          color: #a5d6ff;
        }

        .token-number {
          color: #f2cc60;
        }

        .token-comment {
          color: #8b949e;
          font-style: italic;
        }

        .config-summary {
          background: #ffffff;
          border-radius: 8px;
          padding: 1.5rem;
          box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .config-summary h3 {
          margin: 0 0 1rem 0;
          font-size: 1.1rem;
          color: #1a1a2e;
        }

        .config-summary table {
          width: 100%;
          border-collapse: collapse;
          font-size: 14px;
        }

        .config-summary th,
        .config-summary td {
          padding: 0.75rem 0.5rem;
          text-align: left;
          border-bottom: 1px solid #e9ecef;
        }

        .config-summary th {
          font-weight: 600;
          color: #495057;
          background: #f8f9fa;
        }

        .config-summary td:nth-child(2) {
          font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
          color: #0d6efd;
        }

        .config-summary tr:last-child td {
          border-bottom: none;
        }
      `}</style>
    </section>
  )
}
