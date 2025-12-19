import React from 'react';
import { Features } from './components/Features';
import './App.css';

const App: React.FC = () => {
  return (
    <div className="app">
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
      </main>

      <footer className="footer">
        <div className="container">
          <p>&copy; 2024 MirDB. Built with Rust.</p>
        </div>
      </footer>
    </div>
  );
};

export default App;
