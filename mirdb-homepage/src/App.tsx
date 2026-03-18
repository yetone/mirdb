/**
 * Root application component for MirDB Homepage.
 */

import { Layout } from './components/layout/Layout';
import { Hero } from './components/sections/Hero';
import { QuickStart } from './components/sections/QuickStart';

function App() {
  return (
    <Layout>
      <Hero />
      <QuickStart />
    </Layout>
  );
}

export default App;
