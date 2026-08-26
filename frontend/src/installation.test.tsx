/**
 * The installation banner.
 *
 * One thing is being protected above all others: "the account was read and the
 * resources are gone" and "the account could not be read" must never render as
 * the same screen, and only the first may lead to a button that spends money.
 * A real sandbox sweep produces the second, so the state that most often
 * signals a genuine loss is also the state that must refuse to act on it.
 */

import { cleanup, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { InstallationStatus, RecoveryAttempt } from './api/types'
import { InstallationBanner, bannerCopy } from './installation'

const PHRASE = 'recreate ad-selfheal-001 for $8.35 a day'

function status(overrides: Partial<InstallationStatus> = {}): InstallationStatus {
  return {
    audience: 'operator',
    state: 'verified_present',
    detail: 'all 7 sealed AWS resources are present in the account',
    sealed_resources: 7,
    absent_resources: 0,
    checked: true,
    checked_seconds_ago: 3,
    reason: '',
    deployed: false,
    manifest_status: 'ready',
    manifest_run_id: 'ad-selfheal-001',
    transitional_recovery: '',
    mutation_in_progress: false,
    mutation_holder: '',
    recovery: {
      offered: false,
      code: 'present',
      refusal: 'NOTHING TO RECOVER',
      confirmation_phrase: '',
      usd_per_day: '8.35',
      usd_per_day_basis: 'AWS standing cost for 24 hours at public on-demand rates',
      plan: 'This would re-apply Terraform.',
      attempts_in_window: 0,
      attempts_allowed: 3,
    },
    attempt: null,
    ...overrides,
  }
}

function gone(overrides: Partial<InstallationStatus> = {}): InstallationStatus {
  return status({
    state: 'verified_missing',
    absent_resources: 7,
    detail: 'THE SEALED AWS INFRASTRUCTURE IS GONE',
    recovery: {
      offered: true,
      code: 'offered',
      refusal: '',
      confirmation_phrase: PHRASE,
      usd_per_day: '8.35',
      usd_per_day_basis: 'AWS standing cost for 24 hours at public on-demand rates',
      plan: 'The manifest still says ready, so this would re-apply Terraform and reseed.',
      attempts_in_window: 0,
      attempts_allowed: 3,
    },
    ...overrides,
  })
}

function blind(overrides: Partial<InstallationStatus> = {}): InstallationStatus {
  return status({
    state: 'unverified',
    checked: false,
    reason: 'the AWS credentials could not be loaded (SSOTokenLoadError)',
    recovery: {
      offered: false,
      code: 'unverified',
      refusal: 'RECOVERY IS REFUSED: THE ACCOUNT COULD NOT BE READ',
      confirmation_phrase: '',
      usd_per_day: '8.35',
      usd_per_day_basis: 'AWS standing cost for 24 hours at public on-demand rates',
      plan: 'The manifest still says ready.',
      attempts_in_window: 0,
      attempts_allowed: 3,
    },
    ...overrides,
  })
}

/** The deployed app answering the sealed owner: blind, and nothing to press. */
function deployedOperator(overrides: Partial<InstallationStatus> = {}): InstallationStatus {
  return blind({
    deployed: true,
    recovery: {
      ...blind().recovery,
      code: 'deployed',
      refusal:
        'RECOVERY CANNOT RUN HERE. This is the deployed Databricks App, and its '
        + 'inability to re-create anything is physical rather than a policy: it has no '
        + 'Terraform state, no terraform, aws or databricks binaries, and no manifest '
        + "path at all. Run './antidemo setup' from a checkout on the operator's machine.",
    },
    ...overrides,
  })
}

/**
 * The deployed app answering anybody else -- shaped as the server actually
 * sends it, with every prose field already emptied there rather than here. A
 * fixture that carried the operator text and relied on the component to hide it
 * would be testing a weaker guarantee than the one that ships.
 */
function deployedViewer(overrides: Partial<InstallationStatus> = {}): InstallationStatus {
  return blind({
    audience: 'viewer',
    deployed: true,
    detail: '',
    reason: '',
    transitional_recovery: '',
    mutation_holder: '',
    recovery: {
      offered: false,
      code: 'deployed',
      refusal: '',
      confirmation_phrase: '',
      usd_per_day: '',
      usd_per_day_basis: '',
      plan: '',
      attempts_in_window: 0,
      attempts_allowed: 3,
    },
    attempt: null,
    ...overrides,
  })
}

function attempt(overrides: Partial<RecoveryAttempt> = {}): RecoveryAttempt {
  return {
    attempt_id: '20260822T120000Z',
    phase: 'running',
    detail: 'The installer is running.',
    started_at: '2026-08-22T12:00:00Z',
    finished_at: '',
    exit_code: null,
    pid: 4242,
    log_tail: [],
    ...overrides,
  }
}

/** Serves the banner's reads and records every POST that reached the network. */
function stubApi(payload: InstallationStatus) {
  const posted: Array<{ url: string; body: unknown }> = []
  const fetchMock = vi.fn().mockImplementation((url: string, init?: RequestInit) => {
    if (init?.method === 'POST') {
      posted.push({ url, body: JSON.parse(String(init.body)) })
      return Promise.resolve({
        ok: true,
        status: 202,
        statusText: 'Accepted',
        json: () => Promise.resolve({ attempt_id: 'x', poll: '/api/installation/recovery/x' }),
      })
    }
    return Promise.resolve({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: () => Promise.resolve(payload),
    })
  })
  vi.stubGlobal('fetch', fetchMock)
  return { fetchMock, posted }
}

afterEach(() => {
  cleanup()
  vi.unstubAllGlobals()
})

describe('the four states are four different sentences', () => {
  it('never renders a could-not-look as a confirmed absence', () => {
    const copy = bannerCopy(blind())
    expect(copy?.tone).toBe('blind')
    expect(copy?.title).toBe('THE ACCOUNT COULD NOT BE READ')
    expect(copy?.lede).toContain('NOT a report that anything is missing')
    // The remediation is a credential, not a database.
    expect(copy?.instead).toContain('aws sso login --sso-session databricks-sandbox')
    // And it says why this is also what a genuine sweep looks like, so the
    // refusal is not mistaken for reassurance.
    expect(copy?.instead).toContain('deletes the IAM users as well as the databases')
    expect(copy?.instead).toContain('still is not proof')
  })

  it('says plainly when nobody has looked at all', () => {
    const copy = bannerCopy(status({ state: 'never_checked', checked: false }))
    expect(copy?.title).toBe('NOBODY HAS LOOKED YET')
    expect(copy?.lede).toContain('NOT a report that anything is missing')
    expect(copy?.lede).toContain('NOT a report that anything is fine')
  })

  it('states a confirmed absence as a fact with a count', () => {
    const copy = bannerCopy(gone())
    expect(copy?.tone).toBe('gone')
    expect(copy?.lede).toContain('The account was read')
    expect(copy?.lede).toContain('7 of 7 sealed resources are absent')
  })

  it('does not send the deployed app to a shell it does not have', () => {
    // The deployed app is `unverified` essentially always -- it has no AWS
    // credentials to read the account with -- so this is the deployed screen,
    // not an edge of it. Telling its reader to run `aws sso login` would be
    // advice for a machine they are not sitting at.
    const copy = bannerCopy(deployedOperator())
    expect(copy?.instead).toContain('RECOVERY CANNOT RUN HERE')
    expect(copy?.instead).not.toContain('aws sso login')
  })

  it('shows nothing at all when the account was read and everything is there', () => {
    // A healthy installation gets no banner: this is projected in front of
    // customers, and a permanent bar saying "fine" is noise that teaches
    // operators to ignore the bar.
    expect(bannerCopy(status())).toBeNull()
  })
})

describe('who the banner is written for', () => {
  it('renders nothing at all for a viewer of the deployed app', async () => {
    // The defect: a viewer opened the deployed app and got an expired-token
    // trace, a Terraform state path and a shell command filling the top third
    // of the screen, above the title card. None of it was theirs to act on and
    // all of it was in front of the room.
    expect(bannerCopy(deployedViewer())).toBeNull()

    stubApi(deployedViewer())
    const { container } = render(<InstallationBanner />)
    // Waited for rather than asserted immediately: the first render is empty
    // for everybody, so a bare assertion would pass even if the fetch then
    // painted the wall.
    await waitFor(() => expect(fetch).toHaveBeenCalled())
    expect(container.querySelector('.installation-banner')).toBeNull()
  })

  it('is silent for a viewer whatever the account says', () => {
    // A confirmed absence is the loudest state this surface has, and it is
    // still not a viewer's to read: the round-select screen is what tells them
    // which rounds can run, and it says so whether or not this is rendered.
    expect(bannerCopy(deployedViewer({ state: 'verified_missing', absent_resources: 7 }))).toBeNull()
    expect(bannerCopy(deployedViewer({ state: 'never_checked' }))).toBeNull()
  })

  it('keeps the local operator screen exactly as it was', () => {
    // The path every bout has actually been run on. An operator at the machine
    // that holds the Terraform state gets the whole diagnosis, unfolded.
    const copy = bannerCopy(blind())
    expect(copy?.quiet).toBeUndefined()
    expect(copy?.title).toBe('THE ACCOUNT COULD NOT BE READ')
    expect(copy?.instead).toContain('aws sso login')
  })

  it('falls back to the operator screen when the server names no audience', () => {
    // A browser holding a bundle newer than the server. Silence would be the
    // worse failure: it would take the diagnosis away from a local operator
    // over a version skew, and this screen's whole purpose is to not go quiet.
    const withoutAudience: InstallationStatus = { ...blind() }
    delete withoutAudience.audience
    expect(bannerCopy(withoutAudience)?.title).toBe('THE ACCOUNT COULD NOT BE READ')
  })
})

describe('the deployed app, read by its own operator', () => {
  it('leads with one line about what was checked, not with Terraform', async () => {
    stubApi(deployedOperator())
    render(<InstallationBanner />)

    const lede = await screen.findByText(/could not read the AWS account/)
    expect(lede).toBeInTheDocument()
    // It reports its own reading and stops. Which rounds can run is the
    // round-select screen's verdict, and restating it here is how two surfaces
    // come to disagree.
    expect(lede.textContent).toContain('round-select screen')
    expect(lede.textContent).not.toContain('Terraform')
  })

  it('folds the full diagnosis behind a disclosure rather than deleting it', async () => {
    const user = userEvent.setup()
    stubApi(deployedOperator())
    render(<InstallationBanner />)

    const summary = await screen.findByText('Show the operator diagnosis')
    const details = summary.closest('details')
    expect(details).not.toBeNull()
    // Closed on arrival: this banner is fixed to the top of a projected screen.
    expect(details).not.toHaveAttribute('open')
    // And nothing is lost -- the server's own words are one click away.
    await user.click(summary)
    expect(details).toHaveAttribute('open')
    expect(screen.getByText(/RECOVERY CANNOT RUN HERE/)).toBeInTheDocument()
  })

  it('never offers a spend, because nothing deployed can spend', async () => {
    stubApi(deployedOperator({ state: 'verified_missing', absent_resources: 7 }))
    render(<InstallationBanner />)
    await screen.findByText('Show the operator diagnosis')
    expect(screen.queryByRole('button', { name: /Show what recovery would do/ })).toBeNull()
  })
})

describe('the recovery control', () => {
  it('is absent when the account could not be read', async () => {
    stubApi(blind())
    render(<InstallationBanner />)
    await screen.findByText('THE ACCOUNT COULD NOT BE READ')
    expect(screen.queryByRole('button', { name: /Show what recovery would do/ })).toBeNull()
    expect(screen.queryByRole('button', { name: /Re-create the infrastructure/ })).toBeNull()
  })

  it('is absent when nothing has looked yet', async () => {
    stubApi(status({ state: 'never_checked', checked: false }))
    render(<InstallationBanner />)
    await screen.findByText('NOBODY HAS LOOKED YET')
    expect(screen.queryByRole('button', { name: /Show what recovery would do/ })).toBeNull()
  })

  it('is absent in the deployed app even with a confirmed absence', async () => {
    stubApi(gone({
      deployed: true,
      recovery: { ...gone().recovery, offered: false, code: 'deployed', confirmation_phrase: '' },
    }))
    render(<InstallationBanner />)
    // The deployed operator's note names the count rather than shouting the
    // headline, but the refusal to offer a spend is the same refusal.
    await screen.findByText(/7 of 7 sealed resources are absent/)
    expect(screen.queryByRole('button', { name: /Show what recovery would do/ })).toBeNull()
  })

  it('takes two deliberate acts, and the second cannot be defaulted', async () => {
    const user = userEvent.setup()
    const { posted } = stubApi(gone())
    render(<InstallationBanner />)

    // Act one: nothing is spendable until the operator asks to see the terms.
    const reveal = await screen.findByRole('button', { name: /Show what recovery would do/ })
    expect(screen.queryByRole('button', { name: /Re-create the infrastructure/ })).toBeNull()
    await user.click(reveal)

    // Act two: the phrase is server-issued and has to be typed. The submit is
    // disabled until it matches exactly, and nothing has been posted.
    const submit = screen.getByRole('button', { name: /Re-create the infrastructure/ })
    expect(submit).toBeDisabled()
    const field = screen.getByLabelText(/Type this exactly to confirm/)
    await user.type(field, 'yes')
    expect(submit).toBeDisabled()
    expect(posted).toHaveLength(0)

    await user.clear(field)
    await user.type(field, PHRASE)
    expect(submit).toBeEnabled()
    await user.click(submit)

    await waitFor(() => expect(posted).toHaveLength(1))
    expect(posted[0].url).toBe('/api/installation/recover')
    expect(posted[0].body).toEqual({ confirm: PHRASE })
  })

  it('names what would be created and what it costs before it can be confirmed', async () => {
    const user = userEvent.setup()
    stubApi(gone())
    render(<InstallationBanner />)
    await user.click(await screen.findByRole('button', { name: /Show what recovery would do/ }))

    expect(screen.getByText(/re-apply Terraform and reseed/)).toBeInTheDocument()
    expect(screen.getByText(/about \$8\.35 a day/)).toBeInTheDocument()
    expect(screen.getByText(/0 of 3 recoveries/)).toBeInTheDocument()
    // The durability of the limit is stated where the money is spent.
    expect(screen.getByText(/restarting the server does not/)).toBeInTheDocument()
  })
})

describe('a recovery in flight', () => {
  it('reports progress from the attempt file rather than claiming completion', async () => {
    stubApi(gone({ attempt: attempt({ log_tail: ['terraform apply', 'creating aurora'] }) }))
    render(<InstallationBanner />)

    await screen.findByText('RE-CREATING THE INFRASTRUCTURE')
    const log = screen.getByLabelText('Recovery log')
    expect(within(log).getByText(/creating aurora/)).toBeInTheDocument()
    // No offer while one is running.
    expect(screen.queryByRole('button', { name: /Show what recovery would do/ })).toBeNull()
  })

  it('says an installer that vanished is lost rather than still running', async () => {
    stubApi(gone({
      attempt: attempt({
        phase: 'lost',
        detail: 'The recovery process is gone and never recorded an ending.',
      }),
      recovery: { ...gone().recovery, offered: false, code: 'attempt_running' },
    }))
    render(<InstallationBanner />)
    // Both the phase line and the detail say it, which is deliberate: the
    // phase is what a glance reads and the detail is what an operator acts on.
    expect(await screen.findAllByText(/never recorded an ending/)).toHaveLength(2)
  })
})

describe('when the banner cannot read its own endpoint', () => {
  it('says so rather than falling silent', async () => {
    vi.stubGlobal('fetch', vi.fn().mockRejectedValue(new Error('offline')))
    render(<InstallationBanner />)
    // Silence would render exactly like a healthy installation, which is the
    // defect this whole surface exists to refuse.
    await screen.findByText('THE INSTALLATION STATUS COULD NOT BE READ')
  })
})
