/**
 * Root application component for MirDB Homepage.
 */

import { Layout } from './components/layout/Layout';
import { Hero } from './components/sections/Hero';
import { Features } from './components/sections/Features';
import { UsageExamples } from './components/sections/UsageExamples';
import { QuickStart } from './components/sections/QuickStart';
import { StatusBadges } from './components/sections/StatusBadges';
import './styles/prism-theme.css';

function App() {
  return (
    <Layout>
      <Hero />
      <Features />
      <UsageExamples />
      <QuickStart />
      <StatusBadges />
    </Layout>
  );
}

export default App;
