import { Routes, Route } from 'react-router-dom';
import Layout from './components/layout/Layout';
import Hero from './components/hero/Hero';
import MetricsDashboard from './components/dashboard/MetricsDashboard';
import QueryBuilder from './components/query/QueryBuilder';

function HomePage() {
  return (
    <Layout>
      <Hero />
    </Layout>
  );
}

function DashboardPage() {
  return (
    <Layout>
      <div className="page-content">
        <MetricsDashboard />
      </div>
    </Layout>
  );
}

function BrowserPage() {
  return (
    <Layout>
      <div className="page-content">
        <h1>Key-Value Browser</h1>
        <p>Browse and manage key-value pairs.</p>
      </div>
    </Layout>
  );
}

function ConfigPage() {
  return (
    <Layout>
      <div className="page-content">
        <h1>Configuration</h1>
        <p>View and update server configuration.</p>
      </div>
    </Layout>
  );
}

function DocsPage() {
  return (
    <Layout>
      <div className="page-content">
        <h1>Documentation</h1>
        <p>Protocol reference and code examples.</p>
      </div>
    </Layout>
  );
}

function QueryPage() {
  return (
    <Layout>
      <QueryBuilder />
    </Layout>
  );
}

function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/dashboard" element={<DashboardPage />} />
      <Route path="/browser" element={<BrowserPage />} />
      <Route path="/config" element={<ConfigPage />} />
      <Route path="/docs" element={<DocsPage />} />
      <Route path="/query" element={<QueryPage />} />
    </Routes>
  );
}

export default App;
