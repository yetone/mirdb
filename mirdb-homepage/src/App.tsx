import Layout from './components/Layout';
import Hero from './components/Hero';
import Features from './components/Features';
import Demo from './components/Demo';
import QuickStart from './components/QuickStart';
import Docs from './components/Docs';
import Comparison from './components/Comparison';
import Roadmap from './components/Roadmap';
import GitHubLink from './components/GitHubLink';

export default function App() {
  return (
    <Layout>
      <Hero />
      <Features />
      <Demo />
      <QuickStart />
      <Docs />
      <Comparison />
      <Roadmap />
      <GitHubLink />
    </Layout>
  );
}
