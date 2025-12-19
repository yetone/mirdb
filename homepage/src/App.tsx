import React from 'react';
import { StickyNav } from './components/StickyNav';
import { Features } from './components/Features';
import { QuickStart } from './components/QuickStart';
import { Commands } from './components/Commands';
import { Configuration } from './components/Configuration';
import { GitHubLink } from './components/GitHubLink';
import './App.css';

const App: React.FC = () => {
  return (
    <div className="app">
      <StickyNav />
      <header className="hero">
        <div className="container">
          <h1>MirDB</h1>
          <p className="tagline">Memcached, but persistent.</p>
          <p className="description">
            A high-performance persistent key-value store with memcached protocol compatibility.
            Built with Rust for safety and performance.
          </p>
        </div>
      </header>

      <main>
        <Features />
        <QuickStart />
        <Commands />
        <Configuration />
      </main>

      <footer className="footer">
        <div className="container">
          <p>&copy; 2024 MirDB. Built with Rust.</p>
          <GitHubLink />
        </div>
      </footer>
    </div>
  );
};

export default App;
