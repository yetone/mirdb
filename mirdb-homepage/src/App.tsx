/**
 * Root application component for MirDB Homepage.
 */

import { Layout } from './components/layout/Layout';
import { Hero } from './components/sections/Hero';
import { UsageExamples } from './components/sections/UsageExamples';
import { QuickStart } from './components/sections/QuickStart';
import './styles/prism-theme.css';

function App() {
  return (
    <Layout>
      <Hero />
      <UsageExamples />
      <QuickStart />
    </Layout>
  );
}

export default App;
