import { Features } from '@/components/sections/Features';
import { HowItWorks } from '@/components/sections/HowItWorks';

export default function Home() {
  return (
    <main className="min-h-screen">
      {/* Hero Section - Scenario 1 */}
      <section id="hero" className="py-20 px-4 text-center bg-gradient-to-b from-primary-50 to-white dark:from-slate-900 dark:to-slate-800">
        <h1 className="text-4xl md:text-6xl font-bold text-primary-900 dark:text-white mb-4">
          MirDB
        </h1>
        <p className="text-xl text-gray-600 dark:text-gray-300 max-w-2xl mx-auto">
          Persistent Key-Value Store with Memcached Compatibility
        </p>
      </section>

      {/* Features Section - Scenario 2 */}
      <Features />

      {/* How It Works Section - Scenario 3 */}
      <HowItWorks />

      {/* Status Section - Scenario 4 */}
      <section id="status" className="py-16 px-4 bg-gray-50 dark:bg-slate-800">
        {/* ProjectStatus - Scenario 4 */}
      </section>

      {/* Quick Start Section - Scenario 5 */}
      <section id="quick-start" className="py-16 px-4">
        {/* QuickStart - Scenario 5 */}
      </section>

      {/* Resources Section - Scenario 6 */}
      <section id="resources" className="py-16 px-4 bg-gray-50 dark:bg-slate-800">
        {/* Resources - Scenario 6 */}
      </section>
    </main>
  );
}
