/**
 * Features section with grid of feature cards.
 * Owner: Scenario 4 - Features Section
 *
 * Requirements:
 * - REQ-7: Highlight Memcached protocol, Persistence, LSM Tree,
 *          Skip-list memtable, Multi-level compaction
 */

import { FeatureCard } from '../ui/FeatureCard'
import { features } from '../../config/content'
import './Features.css'

export function Features() {
  return (
    <section className="features section" id="features">
      <div className="container">
        <h2 className="features__heading">Key Features</h2>
        <div className="features__grid">
          {features.map((feature, index) => (
            <FeatureCard
              key={index}
              name={feature.name}
              description={feature.description}
              icon={feature.icon ? <span>{feature.icon}</span> : undefined}
            />
          ))}
        </div>
      </div>
    </section>
  )
}
