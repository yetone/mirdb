import { CodeBlock } from './CodeBlock';
import { QUICK_START } from '../../utils/constants';
import './QuickStart.css';

/**
 * Quick Start Section Component
 * Provides getting started instructions with installation and usage examples.
 */
export function QuickStart() {
  return (
    <section
      id="quick-start"
      className="quick-start section"
      aria-labelledby="quick-start-heading"
    >
      <div className="container">
        <h2 id="quick-start-heading" className="quick-start-heading">
          Quick Start
        </h2>
        <p className="quick-start-intro">
          Get up and running with MirDB in minutes. Follow these simple steps to install and start using your persistent key-value store.
        </p>

        <div className="quick-start-content">
          <div className="quick-start-step">
            <h3 className="quick-start-step-title">Installation</h3>
            <CodeBlock
              code={QUICK_START.installation}
              language="bash"
              title="Terminal"
            />
          </div>

          <div className="quick-start-step">
            <h3 className="quick-start-step-title">Usage</h3>
            <p className="quick-start-step-description">
              MirDB uses the memcached protocol. Connect with any memcached client to perform set and get operations:
            </p>
            <CodeBlock
              code={QUICK_START.usage}
              language="bash"
              title="Memcached Protocol"
            />
          </div>
        </div>
      </div>
    </section>
  );
}
