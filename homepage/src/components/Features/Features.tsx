/**
 * Features section component for MirDB homepage.
 * Owner: Scenario 3 - Features Section Display
 *
 * Requirements:
 * - Display 6 key features in grid layout (REQ-3)
 * - Memcached Protocol Support
 * - Data Persistence via SSTables
 * - LSM Tree Architecture
 * - Async Networking with Tokio
 * - Skip List-based Memtable
 * - Multi-level Compaction
 */

import { features } from '../../data/features'
import { FeatureCard } from './FeatureCard'

export function Features() {
  return (
    <section
      id="features"
      className="py-20 px-4 sm:px-6 lg:px-8"
      aria-labelledby="features-heading"
    >
      <div className="max-w-7xl mx-auto">
        <div className="text-center mb-16">
          <h2
            id="features-heading"
            className="text-3xl sm:text-4xl font-bold text-white mb-4"
          >
            Key Features
          </h2>
          <p className="text-gray-400 max-w-2xl mx-auto text-lg">
            MirDB combines the simplicity of memcached with the durability of persistent storage
          </p>
        </div>

        <div
          className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6"
          data-testid="features-grid"
        >
          {features.map((feature) => (
            <FeatureCard key={feature.title} feature={feature} />
          ))}
        </div>
      </div>
    </section>
  )
}
