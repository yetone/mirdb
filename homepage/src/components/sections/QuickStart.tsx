/**
 * Quick Start section with code examples.
 * Owner: Scenario 4 - Quick Start Section with Code Examples
 */
import React from 'react';
import { Container } from '@/components/common/Container';
import { CodeBlock } from '@/components/ui/CodeBlock';
import { codeExamples } from '@/data/codeExamples';
import styles from './QuickStart.module.css';

export function QuickStart() {
  return (
    <section id="quickstart" className={styles.section} data-testid="quickstart-section">
      <Container>
        <h2 className={styles.heading}>Quick Start</h2>
        <p className={styles.description}>
          Get started with MirDB in minutes. MirDB uses the standard memcached protocol,
          so you can use any existing memcached client.
        </p>

        <div className={styles.examplesGrid}>
          {codeExamples.map((example, index) => (
            <div key={index} className={styles.exampleWrapper}>
              <CodeBlock
                code={example.code}
                language={example.language}
                title={example.title}
              />
            </div>
          ))}
        </div>
      </Container>
    </section>
  );
}
