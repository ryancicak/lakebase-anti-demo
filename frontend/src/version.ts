/**
 * The release this build of the app is, e.g. "1.0.0".
 *
 * Read from package.json when the bundle is built (`define` in vite.config.ts),
 * and tests/test_version.py holds package.json equal to pyproject.toml, so the
 * screen, `/api/version` and the git tag all state one number. It is on the
 * title screen and at the end of the staff roll, which is how to tell which
 * release a running app is. Each fix release bumps it.
 */
export const APP_VERSION: string = __APP_VERSION__

/** How the version is written on screen. */
export const APP_VERSION_LABEL = `v${APP_VERSION}`
