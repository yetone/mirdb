import React from 'react';
import { CodeBlock } from './CodeBlock';

export const installCommand = 'cargo install mirdb';

export const runCommand = 'mirdb -c mirdb.toml';

export const usageExample = `$ telnet localhost 12333
set mykey 0 0 5
hello
STORED
get mykey
VALUE mykey 0 5
hello
END`;

export const configExample = `addr = "0.0.0.0:12333"
max_level = 7
work_dir = "/tmp/mirdb"
sst_max_size = "100M"
mem_table_max_size = "4M"`;

export const QuickStart: React.FC = () => {
  return (
    <section id="quick-start" className="quick-start-section" data-testid="quick-start-section">
      <div className="container">
        <h2 className="section-title">Quick Start</h2>
        <p className="section-description">
          Get MirDB running in under 5 minutes with these simple steps.
        </p>

        <div className="quick-start-steps">
          {/* Step 1: Install */}
          <div className="quick-start-step" data-testid="install-step">
            <h3 className="step-title">
              <span className="step-number">1</span>
              Install MirDB
            </h3>
            <p className="step-description">
              Install MirDB using Cargo, the Rust package manager:
            </p>
            <CodeBlock
              code={installCommand}
              language="bash"
              title="Installation"
            />
          </div>

          {/* Step 2: Configure (optional) */}
          <div className="quick-start-step" data-testid="config-step">
            <h3 className="step-title">
              <span className="step-number">2</span>
              Configure (Optional)
            </h3>
            <p className="step-description">
              Create a configuration file <code>mirdb.toml</code>:
            </p>
            <CodeBlock
              code={configExample}
              language="toml"
              title="mirdb.toml"
            />
          </div>

          {/* Step 3: Run */}
          <div className="quick-start-step" data-testid="run-step">
            <h3 className="step-title">
              <span className="step-number">3</span>
              Run MirDB
            </h3>
            <p className="step-description">
              Start the MirDB server with your configuration:
            </p>
            <CodeBlock
              code={runCommand}
              language="bash"
              title="Start Server"
            />
          </div>

          {/* Step 4: Basic Usage */}
          <div className="quick-start-step" data-testid="usage-step">
            <h3 className="step-title">
              <span className="step-number">4</span>
              Basic Usage
            </h3>
            <p className="step-description">
              Connect using any memcached client or telnet. Here's a SET/GET example:
            </p>
            <CodeBlock
              code={usageExample}
              language="bash"
              title="Example Session"
            />
            <div className="usage-notes">
              <p>
                <strong>SET command:</strong> <code>set &lt;key&gt; &lt;flags&gt; &lt;exptime&gt; &lt;bytes&gt;</code>
              </p>
              <p>
                <strong>Response:</strong> <code>STORED</code> on success
              </p>
              <p>
                <strong>GET command:</strong> <code>get &lt;key&gt;</code>
              </p>
              <p>
                <strong>Response:</strong> <code>VALUE &lt;key&gt; &lt;flags&gt; &lt;bytes&gt;</code> followed by data and <code>END</code>
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
};

export default QuickStart;
