/**
 * Demo Section Component
 * Owner: Scenario 6 - Usage Demonstration
 *
 * Displays usage demonstration:
 * - Terminal-style code example showing set/get operations
 * - Animated visual of MirDB in action
 * - Caption describing the demonstration
 */
import './Demo.css';

interface DemoLine {
  type: 'command' | 'output' | 'comment';
  content: string;
}

const DEMO_CONTENT: DemoLine[] = [
  { type: 'comment', content: '# Connect to MirDB (default port 12333)' },
  { type: 'command', content: '$ telnet localhost 12333' },
  { type: 'output', content: 'Connected to localhost.' },
  { type: 'comment', content: '' },
  { type: 'comment', content: '# Store a value with memcached SET command' },
  { type: 'command', content: 'set mykey 0 0 5' },
  { type: 'command', content: 'hello' },
  { type: 'output', content: 'STORED' },
  { type: 'comment', content: '' },
  { type: 'comment', content: '# Retrieve the value with GET command' },
  { type: 'command', content: 'get mykey' },
  { type: 'output', content: 'VALUE mykey 0 5' },
  { type: 'output', content: 'hello' },
  { type: 'output', content: 'END' },
];

export function Demo() {
  return (
    <section
      id="demo"
      className="demo-section section"
      aria-labelledby="demo-heading"
      data-testid="demo-section"
    >
      <div className="container">
        <h2 id="demo-heading" className="demo-title">See It In Action</h2>
        <p className="demo-subtitle">
          See how easy it is to use MirDB with any memcached client
        </p>

        <figure className="demo-figure" data-testid="demo-figure">
          <div
            className="demo-terminal"
            role="img"
            aria-label="Terminal demonstration showing MirDB set and get operations with memcached protocol"
            data-testid="demo-terminal"
          >
            <div className="terminal-header">
              <span className="terminal-button terminal-close"></span>
              <span className="terminal-button terminal-minimize"></span>
              <span className="terminal-button terminal-maximize"></span>
              <span className="terminal-title">MirDB Terminal Session</span>
            </div>
            <div className="terminal-body">
              {DEMO_CONTENT.map((line, index) => (
                <div
                  key={index}
                  className={`terminal-line terminal-line-${line.type}`}
                  data-testid={`demo-line-${index}`}
                >
                  {line.content}
                </div>
              ))}
            </div>
          </div>
          <figcaption className="demo-caption" data-testid="demo-caption">
            MirDB accepts standard memcached commands like <code>set</code> and <code>get</code>,
            making it compatible with existing memcached client libraries.
          </figcaption>
        </figure>
      </div>
    </section>
  );
}

export default Demo;
