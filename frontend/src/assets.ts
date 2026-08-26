import retroTitle from '../../brand/06-retro-title-screen.svg?url'
import headerRing from './header-ring.svg?url'
// Sprites, not photographs: the 256px photo these replaced never sat right
// beside a roster drawn on the NES grid, and it is gone from the tree.
//
// Both cuts have a TRANSPARENT navy field -- keyed out by a flood fill inward
// from the edges, so navy enclosed by the figure survived -- which is why
// neither slot frames them to contain a background. Each carries its own 1px
// cream keyline, and that is what separates the figure from any backdrop.
// Anything that re-adds a border or a fill here is undoing that.
//
// Two sizes, because the two slots are far apart and pixel art does not
// survive arbitrary rescaling. The commentator bar paints into a fixed 48px
// content box, so it gets a 48px sprite and maps 1:1 with no resampling at
// all; squeezing the 64px one into it dropped every fourth row and broke the
// cream keyline into dashes. The credits run 84-136px, where the extra detail
// in the 64px original is the point.
import ryanPixelSmall from './ryan-pixel-commentator.png?url'
import ryanPixel from './ryan-pixel-portrait.png?url'
import backfillBill from '../../brand/personas-ringside/backfill-bill.svg?url'
import stacktraceJack from '../../brand/personas-ringside/stacktrace-jack.svg?url'
import countQuery from '../../brand/personas-ringside/count-query.svg?url'
import majorPattern from '../../brand/personas-ringside/major-pattern.svg?url'
import doctorDrift from '../../brand/personas-ringside/doctor-drift.svg?url'
import lockjawLucy from '../../brand/personas-ringside/lockjaw-lucy.svg?url'
import threeAmSam from '../../brand/personas-ringside/3am-sam.svg?url'
import theBigWhy from '../../brand/personas-ringside/the-big-why.svg?url'
import cipherViper from '../../brand/personas-ringside/cipher-viper.svg?url'
import launchDayLola from '../../brand/personas-ringside/launch-day-lola.svg?url'
import type { PersonaId } from './api/types'

export const brandAssets = { retroTitle, headerRing, ryanPixel, ryanPixelSmall }

export const personaPortraits: Record<PersonaId, string> = {
  data_engineer: backfillBill,
  software_engineer: stacktraceJack,
  data_analyst: countQuery,
  architect_it: majorPattern,
  data_scientist_ml: doctorDrift,
  dba: lockjawLucy,
  sre: threeAmSam,
  executive: theBigWhy,
  infosec: cipherViper,
  application_owner: launchDayLola,
}
