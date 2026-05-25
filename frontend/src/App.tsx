import HeroSection from './components/hero/HeroSection';
import FeaturesSection from './components/features/FeaturesSection';
import PlannedFeatures from './components/features/PlannedFeatures';
import SystemOverview from './components/system-overview/SystemOverview';
import KVExplorer from './components/kv-explorer/KVExplorer';

const DEFAULT_CONFIG = {
  listen_address: '0.0.0.0:12333',
  max_lsm_levels: 7,
  work_directory: '/tmp/mirdbs',
  sstable_size_mb: 100,
  memtable_size_mb: 4,
};

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-white">
      <main>
        <HeroSection />
        <FeaturesSection />
        <PlannedFeatures />
        <SystemOverview config={DEFAULT_CONFIG} />
        <KVExplorer />
      </main>
    </div>
  );
}

export default App;
