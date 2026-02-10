import { Layout } from './components/layout/Layout'
import { Hero } from './components/sections/Hero'
import { Features } from './components/sections/Features'

function App() {
  return (
    <Layout>
      <Hero />
      <Features />
      {/* UsageExample section will be added by Scenario 3 */}
      {/* GettingStarted section will be added by Scenario 4 */}
    </Layout>
  )
}

export default App
