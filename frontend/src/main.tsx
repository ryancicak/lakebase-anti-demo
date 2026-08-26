import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@fontsource/press-start-2p/400.css'
import App from './App'
import { InstallationBanner } from './installation'
import './styles.css'

// A sibling of the app rather than a child of any screen. Whether the sealed
// infrastructure still exists is an installation-level fact: it is equally true
// on the title screen, mid-bout and on the scorecard, and `App` returns a
// different top-level element for each of those. Mounting it here also keeps it
// alive across every navigation, so a recovery started on one screen keeps
// reporting on the next.
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <InstallationBanner />
    <App />
  </StrictMode>,
)
