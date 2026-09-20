'use strict'

/**
 * Module dependencies.
 */
const util = require('node:util')
const v8 = require('node:v8')
const debug = util.debuglog('koa:application')
const Emitter = require('node:events')
const Stream = require('node:stream')
const http = require('node:http')
const { AsyncLocalStorage } = require('node:async_hooks')

const onFinished = require('on-finished')
const compose = require('koa-compose')
const statuses = require('statuses')
const { HttpError } = require('http-errors')

const request = require('./request')
const response = require('./response')
const context = require('./context')
const isStream = require('./is-stream.js')
const only = require('./only.js')

/** @typedef {typeof import ('./context') & {
 *  app: Application
 *  req: import('http').IncomingMessage
 *  res: import('http').ServerResponse
 *  request: KoaRequest
 *  response: KoaResponse
 *  state: any
 *  originalUrl: string
 * }} Context */

/** @typedef {typeof import('./request')} KoaRequest */

/** @typedef {typeof import('./response')} KoaResponse */

/**
 * Expose `Application` class.
 * Inherits from `Emitter.prototype`.
 */

function getAsyncLocalStorage (options) {
  if (options.asyncLocalStorage instanceof AsyncLocalStorage) {
    return options.asyncLocalStorage
  }
  return new AsyncLocalStorage()
}

module.exports = class Application extends Emitter {
  /**
   * Initialize a new `Application`.
   *
   * @api public
   */

  /**
   *
   * @param {object} [options] Application options
   * @param {string} [options.env='development'] Environment
   * @param {string[]} [options.keys] Signed cookie keys
   * @param {boolean} [options.proxy] Trust proxy headers
   * @param {number} [options.subdomainOffset] Subdomain offset
   * @param {string} [options.proxyIpHeader] Proxy IP header, defaults to X-Forwarded-For
   * @param {number} [options.maxIpsCount] Max IPs read from proxy IP header, default to 0 (means infinity)
   * @param {function} [options.compose] Function to handle middleware composition
   * @param {boolean} [options.asyncLocalStorage] Enable AsyncLocalStorage, default to false
   *
   */

  constructor (options) {
    super()
    options = options || {}
    this.proxy = options.proxy || false
    this.subdomainOffset = options.subdomainOffset || 2
    this.proxyIpHeader = options.proxyIpHeader || 'X-Forwarded-For'
    this.maxIpsCount = options.maxIpsCount || 0
    this.env = options.env || process.env.NODE_ENV || 'development'
    this.compose = options.compose || compose
    if (options.keys) this.keys = options.keys
    this.middleware = []
    this._servers = new Map()
    this._closePromise = null
    this.context = Object.create(context)
    this.request = Object.create(request)
    this.response = Object.create(response)
    // util.inspect.custom support for node 6+
    /* istanbul ignore else */
    if (util.inspect.custom) {
      this[util.inspect.custom] = this.inspect
    }
    if (options.asyncLocalStorage) {
      if (v8.startupSnapshot?.isBuildingSnapshot?.()) {
        this.ctxStorage = null
        v8.startupSnapshot.addDeserializeCallback(({ app, options }) => {
          app.ctxStorage = getAsyncLocalStorage(options)
        }, { app: this, options })
      } else {
        this.ctxStorage = getAsyncLocalStorage(options)
      }
    }
  }

  /**
   * Shorthand for:
   *
   *    http.createServer(app.callback()).listen(...)
   *
   * @param {Mixed} ...
   * @return {import('http').Server}
   * @api public
   */

  listen (...args) {
    debug('listen')
    const server = http.createServer(this.callback())
    this._trackServer(server)
    return server.listen(...args)
  }

  /**
   * Track a server created by `listen()` along with its open
   * connections and in-flight requests, so that `close()` can
   * gracefully drain and close it.
   *
   * @param {import('http').Server} server
   * @api private
   */

  _trackServer (server) {
    const state = { sockets: new Map(), closing: false }
    this._servers.set(server, state)

    server.on('connection', (socket) => {
      state.sockets.set(socket, 0)
      socket.on('close', () => state.sockets.delete(socket))
    })

    server.on('request', (req, res) => {
      const socket = req.socket
      if (!state.sockets.has(socket)) return
      state.sockets.set(socket, state.sockets.get(socket) + 1)
      res.on('finish', () => {
        if (!state.sockets.has(socket)) return
        const pending = state.sockets.get(socket) - 1
        state.sockets.set(socket, pending)
        // while draining, close keep-alive connections as soon as
        // they become idle instead of waiting for keepAliveTimeout
        if (state.closing && pending === 0) socket.destroy()
      })
    })

    server.once('close', () => {
      this._servers.delete(server)
    })
  }

  /**
   * Gracefully close all servers created by `listen()`.
   *
   * Stops accepting new connections, waits for in-flight requests to
   * finish, and closes idle keep-alive connections as they drain.
   * When `timeout` elapses, remaining connections are forcefully
   * destroyed and the returned promise still resolves.
   *
   * @param {object} [options] Close options
   * @param {number} [options.timeout=0] Milliseconds to wait before
   *   forcefully destroying remaining connections. 0 disables the timeout.
   * @return {Promise<void>}
   * @api public
   */

  close (options) {
    options = options || {}
    const timeout = options.timeout === undefined ? 0 : options.timeout
    if (typeof timeout !== 'number' || !Number.isFinite(timeout) || timeout < 0) {
      throw new TypeError('`timeout` must be a non-negative finite number')
    }
    if (this._closePromise) return this._closePromise

    const entries = [...this._servers]
    if (entries.length === 0) return Promise.resolve()

    let timer = null
    if (timeout > 0) {
      timer = setTimeout(() => {
        for (const [server, state] of entries) {
          if (typeof server.closeAllConnections === 'function') {
            server.closeAllConnections()
          }
          for (const socket of state.sockets.keys()) socket.destroy()
        }
      }, timeout)
      timer.unref()
    }

    const closings = entries.map(([server, state]) => this._closeServer(server, state))

    this._closePromise = Promise.all(closings).then(() => {
      if (timer) clearTimeout(timer)
      this._closePromise = null
    })
    return this._closePromise
  }

  /**
   * Close a single tracked server, resolving once it has fully closed.
   *
   * @param {import('http').Server} server
   * @param {object} state
   * @return {Promise<void>}
   * @api private
   */

  _closeServer (server, state) {
    state.closing = true
    return new Promise((resolve) => {
      if (!server.listening) return resolve()
      server.close(() => resolve())
      // close connections that are already idle so an open keep-alive
      // connection cannot delay the close callback
      for (const [socket, pending] of state.sockets) {
        if (pending === 0) socket.destroy()
      }
    })
  }

  /**
   * Return JSON representation.
   * We only bother showing settings.
   *
   * @return {Object}
   * @api public
   */

  toJSON () {
    return only(this, ['subdomainOffset', 'proxy', 'env'])
  }

  /**
   * Inspect implementation.
   *
   * @return {Object}
   * @api public
   */

  inspect () {
    return this.toJSON()
  }

  /**
   * Use the given middleware `fn`.
   *
   * Old-style middleware will be converted.
   *
   * @param {(context: Context) => Promise<any | void>} fn
   * @return {Application} self
   * @api public
   */

  use (fn) {
    if (typeof fn !== 'function') { throw new TypeError('middleware must be a function!') }
    debug('use %s', fn._name || fn.name || '-')
    this.middleware.push(fn)
    return this
  }

  /**
   * Return a request handler callback
   * for node's native http server.
   *
   * @return {Function}
   * @api public
   */

  callback () {
    const fn = this.compose(this.middleware)

    if (!this.listenerCount('error')) this.on('error', this.onerror)

    const handleRequest = (req, res) => {
      const ctx = this.createContext(req, res)
      if (!this.ctxStorage) {
        return this.handleRequest(ctx, fn)
      }
      return this.ctxStorage.run(ctx, async () => {
        return await this.handleRequest(ctx, fn)
      })
    }

    return handleRequest
  }

  /**
   * return current context from async local storage
   */
  get currentContext () {
    if (this.ctxStorage) return this.ctxStorage.getStore()
  }

  /**
   * Handle request in callback.
   *
   * @api private
   */

  handleRequest (ctx, fnMiddleware) {
    const res = ctx.res
    res.statusCode = 404
    const onerror = (err) => ctx.onerror(err)
    const handleResponse = () => respond(ctx)
    onFinished(res, onerror)
    return fnMiddleware(ctx).then(handleResponse).catch(onerror)
  }

  /**
   * Initialize a new context.
   *
   * @api private
   */

  createContext (req, res) {
    /** @type {Context} */
    const context = Object.create(this.context)
    /** @type {KoaRequest} */
    const request = (context.request = Object.create(this.request))
    /** @type {KoaResponse} */
    const response = (context.response = Object.create(this.response))
    context.app = request.app = response.app = this
    context.req = request.req = response.req = req
    context.res = request.res = response.res = res
    request.ctx = response.ctx = context
    request.response = response
    response.request = request
    context.originalUrl = request.originalUrl = req.url
    context.state = {}
    return context
  }

  /**
   * Default error handler.
   *
   * @param {Error} err
   * @api private
   */

  onerror (err) {
    // When dealing with cross-globals a normal `instanceof` check doesn't work properly.
    // See https://github.com/koajs/koa/issues/1466
    // We can probably remove it once jest fixes https://github.com/facebook/jest/issues/2549.
    const isNativeError =
      Object.prototype.toString.call(err) === '[object Error]' ||
      err instanceof Error
    if (!isNativeError) { throw new TypeError(util.format('non-error thrown: %j', err)) }

    if (err.status === 404 || err.expose) return
    if (this.silent) return

    const msg = err.stack || err.toString()
    console.error(`\n${msg.replace(/^/gm, '  ')}\n`)
  }

  /**
   * Help TS users comply to CommonJS, ESM, bundler mismatch.
   * @see https://github.com/koajs/koa/issues/1513
   */

  static get default () {
    return Application
  }
}

/**
 * Response helper.
 */

function respond (ctx) {
  // allow bypassing koa
  if (ctx.respond === false) return

  const res = ctx.res

  if (!ctx.writable) return res.end()

  let body = ctx.body
  const code = ctx.status

  // ignore body
  if (statuses.empty[code]) {
    // strip headers
    ctx.body = null
    return res.end()
  }

  if (ctx.method === 'HEAD') {
    if (!res.headersSent && !ctx.response.has('Content-Length')) {
      const { length } = ctx.response
      if (Number.isInteger(length)) ctx.length = length
    }
    return res.end()
  }

  // status body
  if (body === null || body === undefined) {
    if (ctx.response._explicitNullBody) {
      ctx.response.remove('Content-Type')
      ctx.response.remove('Transfer-Encoding')
      ctx.length = 0
      return res.end()
    }
    if (ctx.req.httpVersionMajor >= 2) {
      body = String(code)
    } else {
      body = ctx.message || String(code)
    }
    if (!res.headersSent) {
      ctx.type = 'text'
      ctx.length = Buffer.byteLength(body)
    }
    return res.end(body)
  }

  // responses

  if (Buffer.isBuffer(body)) return res.end(body)
  if (typeof body === 'string') return res.end(body)

  let stream = null
  if (body instanceof Blob) stream = Stream.Readable.from(body.stream())
  else if (body instanceof ReadableStream) stream = Stream.Readable.from(body)
  else if (body instanceof Response) stream = Stream.Readable.from(body?.body || '')
  else if (isStream(body)) stream = body

  if (stream) {
    return Stream.pipeline(stream, res, err => {
      if (err && ctx.app.listenerCount('error')) ctx.onerror(err)
    })
  }

  // body: json
  body = JSON.stringify(body)
  if (!res.headersSent) {
    ctx.length = Buffer.byteLength(body)
  }
  res.end(body)
}

/**
 * Make HttpError available to consumers of the library so that consumers don't
 * have a direct dependency upon `http-errors`
 */

module.exports.HttpError = HttpError
