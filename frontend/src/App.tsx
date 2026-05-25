import HeroSection from './components/hero/HeroSection';
import FeaturesSection from './components/features/FeaturesSection';
import PlannedFeatures from './components/features/PlannedFeatures';
import SystemOverview from './components/system-overview/SystemOverview';
import { LSMTreeVisualization } from './components/lsm-tree';
import KVExplorer from './components/kv-explorer/KVExplorer';
import { MetricsDashboard } from './components/metrics';

const DEFAULT_CONFIG = {
  listen_address: '0.0.0.0:12333',
  max_lsm_levels: 7,
  work_directory: '/tmp/mirdbs',
  sstable_size_mb: 100,
  memtable_size_mb: 4,
};

const DEMO_LSM_STATE = {
  memtable: { key_count: 50, size_bytes: 4096 },
  immutable_memtable: null,
  levels: [
    { level: 0, file_count: 3, total_size_bytes: 102400 },
    { level: 1, file_count: 2, total_size_bytes: 204800 },
    { level: 2, file_count: 1, total_size_bytes: 512000 },
  ],
};

function App() {
  return (
    <div className="min-h-screen bg-white dark:bg-gray-900 text-gray-900 dark:text-white">
      <main>
        <HeroSection />
        <FeaturesSection />
        <PlannedFeatures />
        <SystemOverview config={DEFAULT_CONFIG} />
        <MetricsDashboard />
        <LSMTreeVisualization state={DEMO_LSM_STATE} />
        <KVExplorer />
      </main>
    </div>
  );
}

export default App;
