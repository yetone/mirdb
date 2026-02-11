/**
 * Quick Start section with installation and usage commands.
 * Owner: Scenario 5 - Quick Start Section
 *
 * Requirements:
 * - REQ-9: Code examples with common usage patterns
 * - REQ-10: Installation and basic commands
 * - US-2: Copy-to-clipboard functionality
 */

import { CodeBlock } from '../ui/CodeBlock'
import { quickStartCommands } from '../../config/content'
import './QuickStart.css'

export function QuickStart() {
  return (
    <section id="quick-start" className="quick-start">
      <div className="container">
        <h2 className="quick-start__heading">Quick Start</h2>
        <p className="quick-start__description">
          Get MirDB up and running in just a few commands.
        </p>

        <div className="quick-start__grid">
          {quickStartCommands.map((command, index) => (
            <div key={index} className="quick-start__block">
              <CodeBlock
                code={command.code}
                language={command.language}
                title={command.title}
              />
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
