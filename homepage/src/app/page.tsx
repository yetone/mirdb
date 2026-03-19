import { HowItWorks } from '@/components/sections/HowItWorks';

export default function Home() {
  return (
    <main className="min-h-screen">
      {/* Hero Section - Scenario 1 */}
      <section id="hero" className="py-20 text-center">
        <h1 className="text-4xl font-bold text-primary-600 dark:text-primary-400">MirDB</h1>
        <p className="text-xl text-slate-600 dark:text-slate-300 mt-4">
          Persistent Key-Value Store with Memcached Compatibility
        </p>
      </section>

      {/* Features Section - Scenario 2 */}
      <section id="features" className="py-16 bg-slate-50 dark:bg-slate-800">
        <div className="max-w-6xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-8">Features</h2>
          <p className="text-center text-slate-600 dark:text-slate-300">Coming soon...</p>
        </div>
      </section>

      {/* How It Works Section - Scenario 3 */}
      <HowItWorks />

      {/* Status Section - Scenario 4 */}
      <section id="status" className="py-16 bg-slate-50 dark:bg-slate-800">
        <div className="max-w-6xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-8">Project Status</h2>
          <p className="text-center text-slate-600 dark:text-slate-300">Coming soon...</p>
        </div>
      </section>

      {/* Quick Start Section - Scenario 5 */}
      <section id="quick-start" className="py-16">
        <div className="max-w-6xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-8">Quick Start</h2>
          <p className="text-center text-slate-600 dark:text-slate-300">Coming soon...</p>
        </div>
      </section>

      {/* Resources Section - Scenario 6 */}
      <section id="resources" className="py-16 bg-slate-50 dark:bg-slate-800">
        <div className="max-w-6xl mx-auto px-4">
          <h2 className="text-3xl font-bold text-center mb-8">Resources</h2>
          <p className="text-center text-slate-600 dark:text-slate-300">Coming soon...</p>
        </div>
      </section>

      {/* Footer - Scenario 7 */}
      <footer className="py-8 border-t border-slate-200 dark:border-slate-700">
        <div className="max-w-6xl mx-auto px-4 text-center text-slate-500 dark:text-slate-400">
          <p>MIT License - MirDB</p>
        </div>
      </footer>
    </main>
  );
}
