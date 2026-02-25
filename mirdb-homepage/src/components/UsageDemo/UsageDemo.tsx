/**
 * Usage Demonstration Component.
 * Owner: Scenario 3 - Usage Demonstration
 *
 * Requirements:
 * - REQ-4: Show basic GET/SET operations
 * - REQ-9: Integrate usage.gif asset
 */

import React from 'react';
import { CodeBlock } from './CodeBlock';
import styles from './UsageDemo.module.css';

const SET_EXAMPLE = `# Connect to MirDB using telnet
telnet localhost 11211

# Set a key-value pair
set mykey 0 0 5
hello
STORED`;

const GET_EXAMPLE = `# Retrieve the value
get mykey
VALUE mykey 0 5
hello
END`;

export function UsageDemo() {
  return (
    <section id="usage" className={styles.usageDemo}>
      <div className={styles.container}>
        <h2 className={styles.title}>Quick Start</h2>
        <p className={styles.subtitle}>
          MirDB uses the Memcached protocol, making it easy to get started with
          familiar commands.
        </p>

        <div className={styles.content}>
          <div className={styles.codeExamples}>
            <CodeBlock
              code={SET_EXAMPLE}
              language="bash"
              title="SET Operation"
            />
            <CodeBlock
              code={GET_EXAMPLE}
              language="bash"
              title="GET Operation"
            />
          </div>

          <div className={styles.demoGif}>
            <img
              src="/assets/usage.gif"
              alt="MirDB usage demonstration showing SET and GET operations in action"
              className={styles.usageGifImage}
            />
          </div>
        </div>
      </div>
    </section>
  );
}
