import { Header } from './components/Header/Header'
import { Hero } from './components/Hero/Hero'
import { Features } from './components/Features/Features'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <section id="features">
          <Features />
        </section>
      </main>
    </div>
  )
}

export default App
