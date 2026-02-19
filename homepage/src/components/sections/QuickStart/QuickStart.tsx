/**
 * Quick Start Section Component
 * Owner: Scenario 4 - Quick Start Section
 *
 * Installation instructions:
 * - Cargo install command
 * - Binary download option
 * - Docker option
 * - Code blocks with copy buttons
 * - Syntax highlighting
 */
import { SECTION_IDS } from '@/utils/constants'
import { CodeBlock } from './CodeBlock'
import styles from './QuickStart.module.css'

export function QuickStart() {
  return (
    <section
      id={SECTION_IDS.quickStart}
      className={styles.quickStart}
      aria-labelledby="quickstart-title"
    >
      <div className={styles.container}>
        <header className={styles.header}>
          <h2 id="quickstart-title" className={styles.title}>
            Quick Start
          </h2>
          <p className={styles.subtitle}>
            Get MirDB up and running in seconds. Choose your preferred installation method.
          </p>
        </header>

        <div className={styles.methods}>
          {/* Cargo Install - Recommended */}
          <article className={styles.method}>
            <h3 className={styles.methodTitle}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={styles.methodIcon}
                aria-hidden="true"
              >
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z" />
                <polyline points="3.27 6.96 12 12.01 20.73 6.96" />
                <line x1="12" y1="22.08" x2="12" y2="12" />
              </svg>
              Cargo Install
              <span className={styles.recommended}>Recommended</span>
            </h3>
            <p className={styles.methodDescription}>
              Install directly from crates.io using Rust's package manager.
            </p>
            <CodeBlock
              code="cargo install mirdb-server"
              language="shell"
              title="Terminal"
            />
          </article>

          {/* Docker */}
          <article className={styles.method}>
            <h3 className={styles.methodTitle}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={styles.methodIcon}
                aria-hidden="true"
              >
                <rect x="2" y="7" width="20" height="14" rx="2" ry="2" />
                <path d="M16 21V5a2 2 0 0 0-2-2h-4a2 2 0 0 0-2 2v16" />
              </svg>
              Docker
            </h3>
            <p className={styles.methodDescription}>
              Run MirDB in a container for easy deployment and isolation.
            </p>
            <CodeBlock
              code="docker run -p 12333:12333 yetone/mirdb"
              language="shell"
              title="Terminal"
            />
          </article>

          {/* Build from Source */}
          <article className={styles.method}>
            <h3 className={styles.methodTitle}>
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className={styles.methodIcon}
                aria-hidden="true"
              >
                <polyline points="16 18 22 12 16 6" />
                <polyline points="8 6 2 12 8 18" />
              </svg>
              Build from Source
            </h3>
            <p className={styles.methodDescription}>
              Clone the repository and build MirDB yourself.
            </p>
            <CodeBlock
              code={`git clone https://github.com/yetone/mirdb.git
cd mirdb
cargo build --release`}
              language="shell"
              title="Terminal"
            />
          </article>
        </div>

        {/* Next Steps */}
        <div className={styles.nextSteps}>
          <h3 className={styles.nextStepsTitle}>Next Steps</h3>
          <div className={styles.nextStepsList}>
            <div className={styles.nextStepItem}>
              <span className={styles.nextStepNumber}>1</span>
              <p className={styles.nextStepText}>
                Start the server: <code>mirdb-server</code>
              </p>
            </div>
            <div className={styles.nextStepItem}>
              <span className={styles.nextStepNumber}>2</span>
              <p className={styles.nextStepText}>
                Connect using any Memcached client on port <code>12333</code>
              </p>
            </div>
            <div className={styles.nextStepItem}>
              <span className={styles.nextStepNumber}>3</span>
              <p className={styles.nextStepText}>
                Start storing data with <code>set</code>, <code>get</code>, and <code>delete</code> commands
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
