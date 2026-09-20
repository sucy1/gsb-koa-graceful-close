'use strict'

const { describe, it } = require('node:test')
const assert = require('node:assert/strict')
const http = require('node:http')
const Koa = require('../..')

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

function listen (app) {
  const server = app.listen()
  return new Promise((resolve) => {
    server.once('listening', () => resolve(server))
  })
}

function request (port, { agent, path = '/' } = {}) {
  return new Promise((resolve, reject) => {
    const req = http.get({ port, agent, path }, (res) => {
      let body = ''
      res.on('data', (chunk) => { body += chunk })
      res.on('end', () => resolve({ res, body }))
    })
    req.on('error', reject)
  })
}

describe('app.close()', () => {
  it('should resolve immediately when no server was created', async () => {
    const app = new Koa()
    await assert.doesNotReject(() => app.close())
    assert.strictEqual(await app.close(), undefined)
  })

  it('should keep listen() returning a native http.Server', async () => {
    const app = new Koa()
    const server = app.listen()
    assert.ok(server instanceof http.Server)
    await app.close()
  })

  it('should throw TypeError for invalid timeout values', async () => {
    const app = new Koa()
    for (const timeout of [-1, NaN, Infinity, -Infinity, '100', true, {}, []]) {
      assert.throws(() => app.close({ timeout }), TypeError)
    }
    await app.close()
  })

  it('should stop accepting new connections', async () => {
    const app = new Koa()
    app.use((ctx) => { ctx.body = 'ok' })
    const server = await listen(app)
    const port = server.address().port

    const { body } = await request(port)
    assert.strictEqual(body, 'ok')

    await app.close()

    // the very first reconnect attempt may race the listen socket
    // teardown and get ECONNRESET, so poll until the port refuses
    let err
    for (let i = 0; i < 50; i++) {
      err = await request(port).catch((e) => e)
      if (err instanceof Error && err.code === 'ECONNREFUSED') break
      await sleep(10)
    }
    assert.ok(err instanceof Error, 'expected connection to fail')
    assert.strictEqual(err.code, 'ECONNREFUSED')
  })

  it('should wait for in-flight requests to fully complete', async () => {
    const app = new Koa()
    app.use(async (ctx) => {
      await sleep(100)
      ctx.body = 'full response body'
    })
    const server = await listen(app)
    const port = server.address().port

    const reqPromise = request(port)
    await sleep(20)
    const closePromise = app.close()

    const { res, body } = await reqPromise
    assert.strictEqual(res.statusCode, 200)
    assert.strictEqual(body, 'full response body')

    await closePromise
    assert.strictEqual(server.listening, false)
  })

  it('should not wait for keepAliveTimeout once connections are idle', async () => {
    const app = new Koa()
    app.use(async (ctx) => {
      await sleep(100)
      ctx.body = 'ok'
    })
    const server = await listen(app)
    server.keepAliveTimeout = 60000
    const port = server.address().port
    const agent = new http.Agent({ keepAlive: true, maxSockets: 1 })

    try {
      // warm up an idle keep-alive connection
      await request(port, { agent })
      assert.strictEqual(Object.keys(agent.freeSockets).length > 0, true)

      // start a slow request on the same connection, close during it
      const reqPromise = request(port, { agent })
      await sleep(20)
      const closePromise = app.close()
      const { body } = await reqPromise
      assert.strictEqual(body, 'ok')

      const drainedAt = Date.now()
      await closePromise
      const elapsed = Date.now() - drainedAt
      assert.ok(elapsed < 500, `close() took ${elapsed}ms after last in-flight request`)
    } finally {
      agent.destroy()
      await app.close()
    }
  })

  it('should close an idle keep-alive connection right away', async () => {
    const app = new Koa()
    app.use((ctx) => { ctx.body = 'ok' })
    const server = await listen(app)
    server.keepAliveTimeout = 60000
    const port = server.address().port
    const agent = new http.Agent({ keepAlive: true, maxSockets: 1 })

    try {
      await request(port, { agent })
      assert.strictEqual(Object.keys(agent.freeSockets).length > 0, true)

      const startedAt = Date.now()
      await app.close()
      const elapsed = Date.now() - startedAt
      assert.ok(elapsed < 500, `close() took ${elapsed}ms with an idle keep-alive connection`)
    } finally {
      agent.destroy()
      await app.close()
    }
  })

  it('should forcefully destroy remaining connections after timeout and still resolve', async () => {
    const app = new Koa()
    app.use(async (ctx) => {
      await new Promise(() => {}) // never responds
    })
    const server = await listen(app)
    const port = server.address().port

    const reqPromise = request(port).catch((err) => err)
    await sleep(20)

    const startedAt = Date.now()
    await app.close({ timeout: 100 })
    const elapsed = Date.now() - startedAt

    assert.ok(elapsed >= 100, `close() resolved too early: ${elapsed}ms`)
    assert.ok(elapsed < 600, `close() resolved too late: ${elapsed}ms`)
    assert.strictEqual(server.listening, false)

    const err = await reqPromise
    assert.ok(err instanceof Error)
  })

  it('should not force-close when timeout is 0', async () => {
    const app = new Koa()
    app.use(async (ctx) => {
      await sleep(100)
      ctx.body = 'ok'
    })
    const server = await listen(app)
    const port = server.address().port

    const reqPromise = request(port)
    await sleep(20)
    await app.close({ timeout: 0 })
    const { body } = await reqPromise
    assert.strictEqual(body, 'ok')
  })

  it('should return the same promise while closing is in progress', async () => {
    const app = new Koa()
    app.use(async (ctx) => {
      await sleep(100)
      ctx.body = 'ok'
    })
    const server = await listen(app)
    const port = server.address().port

    const reqPromise = request(port)
    await sleep(20)

    const p1 = app.close()
    const p2 = app.close()
    assert.strictEqual(p1, p2)

    await p1
    await reqPromise

    // closing again after completion resolves directly
    await assert.doesNotReject(() => app.close())
  })

  it('should allow listen() again after close and close the new server', async () => {
    const app = new Koa()
    app.use((ctx) => { ctx.body = 'ok' })

    const server1 = await listen(app)
    await app.close()
    assert.strictEqual(server1.listening, false)

    const server2 = await listen(app)
    const { body } = await request(server2.address().port)
    assert.strictEqual(body, 'ok')

    await app.close()
    assert.strictEqual(server2.listening, false)
  })

  it('should close multiple servers created by listen()', async () => {
    const app = new Koa()
    app.use((ctx) => { ctx.body = 'ok' })

    const server1 = await listen(app)
    const server2 = await listen(app)

    await app.close()
    assert.strictEqual(server1.listening, false)
    assert.strictEqual(server2.listening, false)
  })
})
