import { Header } from './components/Header/Header'
import { Hero } from './components/Hero/Hero'
import { Features } from './components/Features/Features'
import { Installation } from './components/Installation/Installation'
import { Usage } from './components/Usage/Usage'
import { Footer } from './components/Footer/Footer'

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
        <Usage />
      </main>
      <Footer />
    </div>
  )
}

export default App
