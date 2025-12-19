import { Hero } from './components/Hero'
import { Comparison } from './components/Comparison'
import './App.css'

function App() {
  return (
    <div className="app">
      <Hero
        productName="MirDB"
        tagline="Memcached, but persistent."
        description="A persistent key-value store with memcached protocol compatibility. Built with Rust for safety and performance."
        ctaButtons={[
          { label: 'Get Started', href: '#quickstart', primary: true },
          { label: 'View on GitHub', href: 'https://github.com/example/mirdb', primary: false },
        ]}
      />
      <Comparison
        title="Why MirDB over Memcached?"
        subtitle="All the speed of memcached, with the durability you need"
        persistenceHighlight="Unlike memcached, your data survives restarts. MirDB persists data to disk using SSTables, so you never lose your cached data."
        compatibilityMessage="Drop-in replacement for memcached. Existing clients work without modification - just point them to MirDB."
        items={[
          {
            feature: 'Data Persistence',
            mirdb: 'Yes - data survives restarts',
            memcached: 'No - data lost on restart',
            advantage: true,
          },
          {
            feature: 'Protocol',
            mirdb: 'Memcached protocol compatible',
            memcached: 'Memcached protocol',
            advantage: false,
          },
          {
            feature: 'Storage Engine',
            mirdb: 'LSM-tree with SSTables',
            memcached: 'In-memory only',
            advantage: true,
          },
          {
            feature: 'Existing Clients',
            mirdb: 'Works with all memcached clients',
            memcached: 'Native memcached clients',
            advantage: false,
          },
        ]}
      />
    </div>
  )
}

export default App
