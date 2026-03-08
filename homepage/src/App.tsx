import { Header } from './components/Header'

function App() {
  return (
    <div className="min-h-screen bg-slate-900 text-white">
      <Header />
      <main id="main-content" className="container mx-auto px-4 py-8">
        {/* Other sections will be added by their respective scenario builders */}
      </main>
    </div>
  )
}

export default App
