'use strict'

const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const http = require('node:http')
const Koa = require('../..')

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function listen (app) {
  return new Promise((resolve, reject) => {
    const server = app.listen(0)
    server.once('listening', () => resolve(server))
    server.once('error', reject)
  })
}

function fetch (port, { agent, path = '/' } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.get({ port, path, agent }, (res) => {
      let body = ''
      res.setEncoding('utf8')
      res.on('data', (chunk) => { body += chunk })
      res.on('end', () => resolve({ res, body }))
    })
    req.on('error', reject)
  })
}

function assertConnectionRefused (err) {
  // macOS may report ECONNRESET instead of ECONNREFUSED when the port
  // still has sockets in TIME_WAIT state; both mean the server is closed.
  assert.ok(
    err.code === 'ECONNREFUSED' || err.code === 'ECONNRESET',
    `unexpected error code: ${err.code}`
  )
  return true
}

describe('app.close()', () => {
  it('should resolve immediately when the app never listened', async () => {
    const app = new Koa()
    assert.strictEqual(await app.close(), undefined)
  })

  it('should keep listen() returning a native http.Server closable externally', async () => {
    const app = new Koa()
    app.use(ctx => { ctx.body = 'ok' })
    const server = await listen(app)
    assert.ok(server instanceof http.Server)
    await new Promise((resolve) => server.close(resolve))
    assert.strictEqual(await app.close(), undefined)
  })

  it('should wait for in-flight requests to finish', async () => {
    const app = new Koa()
    let started
    const startedPromise = new Promise((resolve) => { started = resolve })
    app.use(async (ctx) => {
      started()
      await delay(200)
      ctx.body = 'complete body'
    })
    const server = await listen(app)
    const port = server.address().port

    const pending = fetch(port)
    await startedPromise
    const closing = app.close()

    const { body } = await pending
    assert.strictEqual(body, 'complete body')
    assert.strictEqual(await closing, undefined)
    assert.strictEqual(server.listening, false)
  })

  it('should refuse new connections once closed', async () => {
    const app = new Koa()
    app.use(ctx => { ctx.body = 'ok' })
    const server = await listen(app)
    const port = server.address().port

    await app.close()

    await assert.rejects(fetch(port), assertConnectionRefused)
  })

  it('should not wait for keepAliveTimeout once connections become idle', async () => {
    const app = new Koa()
    let slowStarted
    const slowStartedPromise = new Promise((resolve) => { slowStarted = resolve })
    app.use(async (ctx) => {
      if (ctx.path === '/slow') {
        slowStarted()
        await delay(300)
        ctx.body = 'slow'
      } else {
        ctx.body = 'fast'
      }
    })
    const server = await listen(app)
    const port = server.address().port
    const agent = new http.Agent({ keepAlive: true })

    // establish a keep-alive connection
    const first = await fetch(port, { agent })
    assert.strictEqual(first.body, 'fast')

    // start a slow request on the same connection, close while in-flight
    const slow = fetch(port, { agent, path: '/slow' })
    await slowStartedPromise
    const closing = app.close()

    const { body } = await slow
    assert.strictEqual(body, 'slow')
    const drainedAt = Date.now()
    assert.strictEqual(await closing, undefined)
    const elapsed = Date.now() - drainedAt
    assert.ok(elapsed < 500, `close() took ${elapsed}ms after the last in-flight request`)

    agent.destroy()
  })

  it('should force close remaining connections after timeout and still resolve', async () => {
    const app = new Koa()
    let started
    const startedPromise = new Promise((resolve) => { started = resolve })
    app.use(async (ctx) => {
      started()
      await new Promise(() => {}) // never responds
    })
    const server = await listen(app)
    const port = server.address().port

    const req = http.get({ port }, () => {})
    req.on('error', () => {})
    await startedPromise

    const start = Date.now()
    const result = await app.close({ timeout: 150 })
    const elapsed = Date.now() - start

    assert.strictEqual(result, undefined)
    assert.ok(elapsed >= 100, `resolved too early (${elapsed}ms)`)
    assert.ok(elapsed < 650, `resolved too late (${elapsed}ms)`)
    assert.strictEqual(server.listening, false)
  })

  it('should wait forever when timeout is 0', async () => {
    const app = new Koa()
    let started
    const startedPromise = new Promise((resolve) => { started = resolve })
    let release
    const gate = new Promise((resolve) => { release = resolve })
    app.use(async (ctx) => {
      started()
      await gate
      ctx.body = 'ok'
    })
    const server = await listen(app)
    const port = server.address().port

    const pending = fetch(port)
    await startedPromise
    let resolved = false
    const closing = app.close({ timeout: 0 }).then(() => { resolved = true })
    await delay(200)
    assert.strictEqual(resolved, false)
    release()
    await closing
    const { body } = await pending
    assert.strictEqual(body, 'ok')
  })

  it('should throw TypeError for invalid timeout values', async () => {
    const app = new Koa()
    for (const timeout of [-1, NaN, Infinity, -Infinity, '100', 'foo', {}, [], null, true]) {
      assert.throws(() => app.close({ timeout }), TypeError, `timeout: ${String(timeout)}`)
    }
  })

  it('should return the same promise while closing is in progress', async () => {
    const app = new Koa()
    let started
    const startedPromise = new Promise((resolve) => { started = resolve })
    let release
    const gate = new Promise((resolve) => { release = resolve })
    app.use(async (ctx) => {
      started()
      await gate
      ctx.body = 'ok'
    })
    const server = await listen(app)
    const port = server.address().port

    const pending = fetch(port)
    await startedPromise
    const p1 = app.close()
    const p2 = app.close()
    assert.strictEqual(p1, p2)
    release()
    await p1
    const { body } = await pending
    assert.strictEqual(body, 'ok')

    // closing again after completion resolves directly
    assert.strictEqual(await app.close(), undefined)
  })

  it('should close all servers created by listen()', async () => {
    const app = new Koa()
    app.use(ctx => { ctx.body = 'ok' })
    const server1 = await listen(app)
    const server2 = await listen(app)
    const port1 = server1.address().port
    const port2 = server2.address().port

    await app.close()

    assert.strictEqual(server1.listening, false)
    assert.strictEqual(server2.listening, false)
    await assert.rejects(fetch(port1), assertConnectionRefused)
    await assert.rejects(fetch(port2), assertConnectionRefused)
  })

  it('should allow listening again after close and close the new server', async () => {
    const app = new Koa()
    app.use(ctx => { ctx.body = 'ok' })

    await listen(app)
    await app.close()

    const server2 = await listen(app)
    const port = server2.address().port
    const { body } = await fetch(port)
    assert.strictEqual(body, 'ok')

    await app.close()
    assert.strictEqual(server2.listening, false)
    await assert.rejects(fetch(port), assertConnectionRefused)
  })
})
