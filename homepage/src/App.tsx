import { Header } from './components/Header/Header'
import { Hero } from './components/Hero/Hero'
import { Features } from './components/Features/Features'
import { Installation } from './components/Installation/Installation'

function App() {
  return (
    <div className="app">
      <Header />
      <main>
        <Hero />
        <section id="features">
          <Features />
        </section>
        <Installation />
        {/* Usage component - Scenario 4 */}
        {/* Footer component - Scenario 6 */}
      </main>
    </div>
  )
}

export default App
