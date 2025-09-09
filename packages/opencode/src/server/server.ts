import { Log } from "../util/log"
import { Bus } from "../bus"
import { describeRoute, generateSpecs, openAPISpecs } from "hono-openapi"
import { Hono } from "hono"
import { streamSSE } from "hono/streaming"
import { Session } from "../session"
import { resolver, validator as zValidator } from "hono-openapi/zod"
import { z } from "zod"
import { Provider } from "../provider/provider"
import { App } from "../app/app"
import { mapValues } from "remeda"
import { NamedError } from "../util/error"
import { ModelsDev } from "../provider/models"
import { Ripgrep } from "../file/ripgrep"
import { Config } from "../config/config"
import { File } from "../file"
import { LSP } from "../lsp"
import { MessageV2 } from "../session/message-v2"
import { callTui, TuiRoute } from "./tui"
import { Permission } from "../permission"
import { lazy } from "../util/lazy"
import { Agent } from "../agent/agent"

const ERRORS = {
  400: {
    description: "Bad request",
    content: {
      "application/json": {
        schema: resolver(
          z
            .object({
              data: z.record(z.string(), z.any()),
            })
            .openapi({
              ref: "Error",
            }),
        ),
      },
    },
  },
} as const

export namespace Server {
  const log = Log.create({ service: "server" })

  export type Routes = ReturnType<typeof app>

  export const Event = {
    Connected: Bus.event("server.connected", z.object({})),
    SessionEvent: Bus.event("session.event", z.object({
      sessionId: z.string(),
      type: z.string(),
      data: z.any(),
    })),
    AgentSwitched: Bus.event("agent.switched", z.object({
      sessionId: z.string(),
      previousAgent: z.string(),
      currentAgent: z.string(),
      trigger: z.string().optional(), // 'auto_signal', 'manual', etc.
      timestamp: z.string(),
    })),
    AgentStatus: Bus.event("agent.status", z.object({
      sessionId: z.string(),
      agent: z.string(),
      status: z.string(), // 'planning', 'executing', 'analyzing', 'idle'
      message: z.string().optional(),
      timestamp: z.string(),
    })),
    ToolUsage: Bus.event("tool.usage", z.object({
      sessionId: z.string(),
      agent: z.string(),
      tool: z.string(),
      action: z.string(), // 'start', 'progress', 'complete', 'error'
      description: z.string().optional(),
      data: z.any().optional(),
      timestamp: z.string(),
    })),
    MessageProgress: Bus.event("message.progress", z.object({
      sessionId: z.string(),
      messageId: z.string().optional(),
      progress: z.number().optional(), // 0-100
      status: z.string(),
      data: z.any().optional(),
      timestamp: z.string(),
    })),
  }

  // Helper functions for emitting agent and tool events
  export const emitAgentStatus = (sessionId: string, agent: string, status: string, message?: string) => {
    Bus.emit(Event.AgentStatus, {
      sessionId,
      agent,
      status,
      message,
      timestamp: new Date().toISOString(),
    })
  }

  export const emitToolUsage = (sessionId: string, agent: string, tool: string, action: string, description?: string, data?: any) => {
    Bus.emit(Event.ToolUsage, {
      sessionId,
      agent,
      tool,
      action,
      description,
      data,
      timestamp: new Date().toISOString(),
    })
  }

  export const emitMessageProgress = (sessionId: string, messageId: string | undefined, status: string, progress?: number, data?: any) => {
    Bus.emit(Event.MessageProgress, {
      sessionId,
      messageId,
      progress,
      status,
      data,
      timestamp: new Date().toISOString(),
    })
  }

  export const app = lazy(() => {
    const app = new Hono()

    const result = app
      .onError((err, c) => {
        if (err instanceof NamedError) {
          return c.json(err.toObject(), {
            status: 400,
          })
        }
        return c.json(new NamedError.Unknown({ message: err.toString() }).toObject(), {
          status: 400,
        })
      })
      .use(async (c, next) => {
        const skipLogging = c.req.path === "/log"
        if (!skipLogging) {
          log.info("request", {
            method: c.req.method,
            path: c.req.path,
          })
        }
        const start = Date.now()
        await next()
        if (!skipLogging) {
          log.info("response", {
            duration: Date.now() - start,
          })
        }
      })
      .get(
        "/doc",
        openAPISpecs(app, {
          documentation: {
            info: {
              title: "opencode",
              version: "0.0.3",
              description: "opencode api",
            },
            openapi: "3.0.0",
          },
        }),
      )
      .get(
        "/event",
        describeRoute({
          description: "Get events",
          operationId: "event.subscribe",
          responses: {
            200: {
              description: "Event stream",
              content: {
                "application/json": {
                  schema: resolver(
                    Bus.payloads().openapi({
                      ref: "Event",
                    }),
                  ),
                },
              },
            },
          },
        }),
        async (c) => {
          log.info("event connected")
          return streamSSE(c, async (stream) => {
            stream.writeSSE({
              data: JSON.stringify({
                type: "server.connected",
                properties: {},
              }),
            })
            const unsub = Bus.subscribeAll(async (event) => {
              await stream.writeSSE({
                data: JSON.stringify(event),
              })
            })
            await new Promise<void>((resolve) => {
              stream.onAbort(() => {
                unsub()
                resolve()
                log.info("event disconnected")
              })
            })
          })
        },
      )
      .get(
        "/event/session/:sessionId",
        describeRoute({
          description: "Get events for a specific session",
          operationId: "event.subscribeSession",
          responses: {
            200: {
              description: "Session-specific event stream",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      type: z.string(),
                      sessionId: z.string(),
                      data: z.any(),
                      timestamp: z.string(),
                    }).openapi({
                      ref: "SessionEvent",
                    }),
                  ),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            sessionId: z.string().openapi({ description: "Session ID to subscribe to" }),
          }),
        ),
        async (c) => {
          const { sessionId } = c.req.valid("param")
          log.info("session event connected", { sessionId })

          return streamSSE(c, async (stream) => {
            // Send initial connection confirmation
            stream.writeSSE({
              data: JSON.stringify({
                type: "session.connected",
                sessionId,
                timestamp: new Date().toISOString(),
                data: {}
              }),
            })

            // Subscribe to all events and filter for this session
            const unsub = Bus.subscribeAll(async (event) => {
              // Filter events related to this session
              const isSessionEvent = (
                // Direct session events
                event.type?.includes("session") &&
                (event.properties?.sessionId === sessionId || event.properties?.sessionID === sessionId)
              ) || (
                // Message events for this session
                event.type?.includes("message") &&
                (event.properties?.sessionId === sessionId || event.properties?.sessionID === sessionId)
              ) || (
                // Chat events for this session
                event.type?.includes("chat") &&
                (event.properties?.sessionId === sessionId || event.properties?.sessionID === sessionId)
              ) || (
                // Tool events for this session
                event.type?.includes("tool") &&
                (event.properties?.sessionId === sessionId || event.properties?.sessionID === sessionId)
              ) || (
                // Agent events for this session
                event.type?.includes("agent") &&
                (event.properties?.sessionId === sessionId || event.properties?.sessionID === sessionId)
              )

              if (isSessionEvent) {
                await stream.writeSSE({
                  data: JSON.stringify({
                    type: event.type,
                    sessionId: sessionId,
                    timestamp: new Date().toISOString(),
                    data: event.properties || event
                  }),
                })
              }
            })

            // Handle client disconnect
            await new Promise<void>((resolve) => {
              stream.onAbort(() => {
                unsub()
                resolve()
                log.info("session event disconnected", { sessionId })
              })
            })
          })
        },
      )
      .get(
        "/app",
        describeRoute({
          description: "Get app info",
          operationId: "app.get",
          responses: {
            200: {
              description: "200",
              content: {
                "application/json": {
                  schema: resolver(App.Info),
                },
              },
            },
          },
        }),
        async (c) => {
          return c.json(App.info())
        },
      )
      .post(
        "/app/init",
        describeRoute({
          description: "Initialize the app",
          operationId: "app.init",
          responses: {
            200: {
              description: "Initialize the app",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => {
          await App.initialize()
          return c.json(true)
        },
      )
      .get(
        "/config",
        describeRoute({
          description: "Get config info",
          operationId: "config.get",
          responses: {
            200: {
              description: "Get config info",
              content: {
                "application/json": {
                  schema: resolver(Config.Info),
                },
              },
            },
          },
        }),
        async (c) => {
          return c.json(await Config.get())
        },
      )
      .get(
        "/session",
        describeRoute({
          description: "List all sessions",
          operationId: "session.list",
          responses: {
            200: {
              description: "List of sessions",
              content: {
                "application/json": {
                  schema: resolver(Session.Info.array()),
                },
              },
            },
          },
        }),
        async (c) => {
          const sessions = await Array.fromAsync(Session.list())
          sessions.sort((a, b) => b.time.updated - a.time.updated)
          return c.json(sessions)
        },
      )
      .get(
        "/session/:id",
        describeRoute({
          description: "Get session",
          operationId: "session.get",
          responses: {
            200: {
              description: "Get session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          const sessionID = c.req.valid("param").id
          const session = await Session.get(sessionID)
          return c.json(session)
        },
      )
      .post(
        "/session",
        describeRoute({
          description: "Create a new session",
          operationId: "session.create",
          responses: {
            ...ERRORS,
            200: {
              description: "Successfully created session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        async (c) => {
          const session = await Session.create()
          return c.json(session)
        },
      )
      .delete(
        "/session/:id",
        describeRoute({
          description: "Delete a session and all its data",
          operationId: "session.delete",
          responses: {
            200: {
              description: "Successfully deleted session",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          await Session.remove(c.req.valid("param").id)
          return c.json(true)
        },
      )
      .post(
        "/session/:id/init",
        describeRoute({
          description: "Analyze the app and create an AGENTS.md file",
          operationId: "session.init",
          responses: {
            200: {
              description: "200",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        zValidator(
          "json",
          z.object({
            messageID: z.string(),
            providerID: z.string(),
            modelID: z.string(),
          }),
        ),
        async (c) => {
          const sessionID = c.req.valid("param").id
          const body = c.req.valid("json")
          await Session.initialize({ ...body, sessionID })
          return c.json(true)
        },
      )
      .post(
        "/session/:id/abort",
        describeRoute({
          description: "Abort a session",
          operationId: "session.abort",
          responses: {
            200: {
              description: "Aborted session",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          return c.json(Session.abort(c.req.valid("param").id))
        },
      )
      .post(
        "/session/:id/share",
        describeRoute({
          description: "Share a session",
          operationId: "session.share",
          responses: {
            200: {
              description: "Successfully shared session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          const id = c.req.valid("param").id
          await Session.share(id)
          const session = await Session.get(id)
          return c.json(session)
        },
      )
      .delete(
        "/session/:id/share",
        describeRoute({
          description: "Unshare the session",
          operationId: "session.unshare",
          responses: {
            200: {
              description: "Successfully unshared session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          const id = c.req.valid("param").id
          await Session.unshare(id)
          const session = await Session.get(id)
          return c.json(session)
        },
      )
      .post(
        "/session/:id/summarize",
        describeRoute({
          description: "Summarize the session",
          operationId: "session.summarize",
          responses: {
            200: {
              description: "Summarized session",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        zValidator(
          "json",
          z.object({
            providerID: z.string(),
            modelID: z.string(),
          }),
        ),
        async (c) => {
          const id = c.req.valid("param").id
          const body = c.req.valid("json")
          await Session.summarize({ ...body, sessionID: id })
          return c.json(true)
        },
      )
      .get(
        "/session/:id/message",
        describeRoute({
          description: "List messages for a session",
          operationId: "session.messages",
          responses: {
            200: {
              description: "List of messages",
              content: {
                "application/json": {
                  schema: resolver(
                    z
                      .object({
                        info: MessageV2.Info,
                        parts: MessageV2.Part.array(),
                      })
                      .array(),
                  ),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        async (c) => {
          const messages = await Session.messages(c.req.valid("param").id)
          return c.json(messages)
        },
      )
      .get(
        "/session/:id/message/:messageID",
        describeRoute({
          description: "Get a message from a session",
          operationId: "session.message",
          responses: {
            200: {
              description: "Message",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      info: MessageV2.Info,
                      parts: MessageV2.Part.array(),
                    }),
                  ),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
            messageID: z.string().openapi({ description: "Message ID" }),
          }),
        ),
        async (c) => {
          const params = c.req.valid("param")
          const message = await Session.getMessage(params.id, params.messageID)
          return c.json(message)
        },
      )
      .post(
        "/session/:id/message",
        describeRoute({
          description: "Create and send a new message to a session",
          operationId: "session.chat",
          responses: {
            200: {
              description: "Created message",
              content: {
                "application/json": {
                  schema: resolver(MessageV2.Assistant),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        zValidator("json", Session.ChatInput.omit({ sessionID: true })),
        async (c) => {
          const sessionID = c.req.valid("param").id
          const body = c.req.valid("json")
          const msg = await Session.chat({ ...body, sessionID })
          return c.json(msg)
        },
      )
      .post(
        "/session/:id/revert",
        describeRoute({
          description: "Revert a message",
          operationId: "session.revert",
          responses: {
            200: {
              description: "Updated session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        zValidator("json", Session.RevertInput.omit({ sessionID: true })),
        async (c) => {
          const id = c.req.valid("param").id
          log.info("revert", c.req.valid("json"))
          const session = await Session.revert({ sessionID: id, ...c.req.valid("json") })
          return c.json(session)
        },
      )
      .post(
        "/session/:id/unrevert",
        describeRoute({
          description: "Restore all reverted messages",
          operationId: "session.unrevert",
          responses: {
            200: {
              description: "Updated session",
              content: {
                "application/json": {
                  schema: resolver(Session.Info),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
          }),
        ),
        async (c) => {
          const id = c.req.valid("param").id
          const session = await Session.unrevert({ sessionID: id })
          return c.json(session)
        },
      )
      .post(
        "/session/:id/permissions/:permissionID",
        describeRoute({
          description: "Respond to a permission request",
          responses: {
            200: {
              description: "Permission processed successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string(),
            permissionID: z.string(),
          }),
        ),
        zValidator("json", z.object({ response: Permission.Response })),
        async (c) => {
          const params = c.req.valid("param")
          const id = params.id
          const permissionID = params.permissionID
          Permission.respond({ sessionID: id, permissionID, response: c.req.valid("json").response })
          return c.json(true)
        },
      )
      .get(
        "/config/providers",
        describeRoute({
          description: "List all providers",
          operationId: "config.providers",
          responses: {
            200: {
              description: "List of providers",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      providers: ModelsDev.Provider.array(),
                      default: z.record(z.string(), z.string()),
                    }),
                  ),
                },
              },
            },
          },
        }),
        async (c) => {
          const providers = await Provider.list().then((x) => mapValues(x, (item) => item.info))
          return c.json({
            providers: Object.values(providers),
            default: mapValues(providers, (item) => Provider.sort(Object.values(item.models))[0].id),
          })
        },
      )
      .get(
        "/find",
        describeRoute({
          description: "Find text in files",
          operationId: "find.text",
          responses: {
            200: {
              description: "Matches",
              content: {
                "application/json": {
                  schema: resolver(Ripgrep.Match.shape.data.array()),
                },
              },
            },
          },
        }),
        zValidator(
          "query",
          z.object({
            pattern: z.string(),
          }),
        ),
        async (c) => {
          const app = App.info()
          const pattern = c.req.valid("query").pattern
          const result = await Ripgrep.search({
            cwd: app.path.cwd,
            pattern,
            limit: 10,
          })
          return c.json(result)
        },
      )
      .get(
        "/find/file",
        describeRoute({
          description: "Find files",
          operationId: "find.files",
          responses: {
            200: {
              description: "File paths",
              content: {
                "application/json": {
                  schema: resolver(z.string().array()),
                },
              },
            },
          },
        }),
        zValidator(
          "query",
          z.object({
            query: z.string(),
          }),
        ),
        async (c) => {
          const query = c.req.valid("query").query
          const app = App.info()
          const result = await Ripgrep.files({
            cwd: app.path.cwd,
            query,
            limit: 10,
          })
          return c.json(result)
        },
      )
      .get(
        "/find/symbol",
        describeRoute({
          description: "Find workspace symbols",
          operationId: "find.symbols",
          responses: {
            200: {
              description: "Symbols",
              content: {
                "application/json": {
                  schema: resolver(LSP.Symbol.array()),
                },
              },
            },
          },
        }),
        zValidator(
          "query",
          z.object({
            query: z.string(),
          }),
        ),
        async (c) => {
          const query = c.req.valid("query").query
          const result = await LSP.workspaceSymbol(query)
          return c.json(result)
        },
      )
      .get(
        "/file",
        describeRoute({
          description: "Read a file",
          operationId: "file.read",
          responses: {
            200: {
              description: "File content",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      type: z.enum(["raw", "patch"]),
                      content: z.string(),
                    }),
                  ),
                },
              },
            },
          },
        }),
        zValidator(
          "query",
          z.object({
            path: z.string(),
          }),
        ),
        async (c) => {
          const path = c.req.valid("query").path
          const content = await File.read(path)
          log.info("read file", {
            path,
            content: content.content,
          })
          return c.json(content)
        },
      )
      .get(
        "/file/status",
        describeRoute({
          description: "Get file status",
          operationId: "file.status",
          responses: {
            200: {
              description: "File status",
              content: {
                "application/json": {
                  schema: resolver(File.Info.array()),
                },
              },
            },
          },
        }),
        async (c) => {
          const content = await File.status()
          return c.json(content)
        },
      )
      .post(
        "/log",
        describeRoute({
          description: "Write a log entry to the server logs",
          operationId: "app.log",
          responses: {
            200: {
              description: "Log entry written successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "json",
          z.object({
            service: z.string().openapi({ description: "Service name for the log entry" }),
            level: z.enum(["debug", "info", "error", "warn"]).openapi({ description: "Log level" }),
            message: z.string().openapi({ description: "Log message" }),
            extra: z
              .record(z.string(), z.any())
              .optional()
              .openapi({ description: "Additional metadata for the log entry" }),
          }),
        ),
        async (c) => {
          const { service, level, message, extra } = c.req.valid("json")
          const logger = Log.create({ service })

          switch (level) {
            case "debug":
              logger.debug(message, extra)
              break
            case "info":
              logger.info(message, extra)
              break
            case "error":
              logger.error(message, extra)
              break
            case "warn":
              logger.warn(message, extra)
              break
          }

          return c.json(true)
        },
      )
      .get(
        "/agent",
        describeRoute({
          description: "List all agents",
          operationId: "app.agents",
          responses: {
            200: {
              description: "List of agents",
              content: {
                "application/json": {
                  schema: resolver(Agent.Info.array()),
                },
              },
            },
          },
        }),
        async (c) => {
          const modes = await Agent.list()
          return c.json(modes)
        },
      )
      .get(
        "/session/:id/agent",
        describeRoute({
          description: "Get current agent for a session",
          operationId: "session.getCurrentAgent",
          responses: {
            200: {
              description: "Current agent information",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      currentAgent: z.string(),
                      availableAgents: z.array(z.string()),
                      sessionTokens: z.number().optional(),
                    })
                  ),
                },
              },
            },
            404: ERRORS[400],
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        async (c) => {
          const sessionId = c.req.valid("param").id
          const session = await Session.get(sessionId)
          
          if (!session) {
            return c.json({ error: "Session not found" }, 404)
          }

          const agents = await Agent.list()
          const availableAgents = agents.map(agent => agent.name)
          
          // Get current agent from session context or default to 'probe'
          const currentAgent = session.context?.currentAgent || 'probe'
          
          // Get session token count if available
          let sessionTokens
          try {
            sessionTokens = session.messages.reduce((total, msg) => {
              return total + (msg.tokens_used || 0)
            }, 0)
          } catch (e) {
            sessionTokens = undefined
          }

          return c.json({
            currentAgent,
            availableAgents,
            sessionTokens,
          })
        },
      )
      .get(
        "/session/:id/tokens",
        describeRoute({
          description: "Get session token count",
          operationId: "session.getTokens",
          responses: {
            200: {
              description: "Session token count",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      sessionId: z.string(),
                      totalTokens: z.number(),
                      messageCount: z.number(),
                    })
                  ),
                },
              },
            },
            404: ERRORS[400],
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        async (c) => {
          const sessionId = c.req.valid("param").id
          const session = await Session.get(sessionId)
          
          if (!session) {
            return c.json({ error: "Session not found" }, 404)
          }

          // Calculate total tokens used in session
          let totalTokens = 0
          let messageCount = 0
          
          try {
            messageCount = session.messages?.length || 0
            totalTokens = session.messages?.reduce((total, msg) => {
              return total + (msg.tokens_used || 0)
            }, 0) || 0
          } catch (e) {
            log.warn("Failed to calculate session tokens", { sessionId, error: e.message })
            totalTokens = 0
          }

          return c.json({
            sessionId,
            totalTokens,
            messageCount,
          })
        },
      )
      .get(
        "/session/:id/usage",
        describeRoute({
          description: "Get detailed session usage data for cost tracking",
          operationId: "session.getUsage",
          responses: {
            200: {
              description: "Session usage data",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      sessionId: z.string(),
                      totalCostUsd: z.number(),
                      totalInputTokens: z.number(),
                      totalOutputTokens: z.number(),
                      totalCachedTokens: z.number(),
                      totalRequests: z.number(),
                      stages: z.record(z.object({
                        cost: z.number(),
                        inputTokens: z.number(),
                        outputTokens: z.number(),
                        requests: z.number(),
                        models: z.record(z.any())
                      })),
                      models: z.record(z.object({
                        cost: z.number(),
                        tokens: z.number(),
                        requests: z.number()
                      })),
                      messageCount: z.number(),
                      lastUpdated: z.string()
                    })
                  ),
                },
              },
            },
            404: ERRORS[400],
          },
        }),
        async (c) => {
          const sessionId = c.req.param("id")
          
          try {
            const session = await Session.get(sessionId)
            
            if (!session) {
              return c.json({ error: "Session not found" }, 404)
            }

            // Initialize usage tracking data
            let usageData = {
              sessionId,
              totalCostUsd: 0,
              totalInputTokens: 0,
              totalOutputTokens: 0,
              totalCachedTokens: 0,
              totalRequests: 0,
              stages: {} as Record<string, any>,
              models: {} as Record<string, any>,
              messageCount: session.messages?.length || 0,
              lastUpdated: new Date().toISOString()
            };

            // Process all messages to extract detailed usage
            if (session.messages) {
              for (const message of session.messages) {
                if (message.role === 'assistant' && message.parts) {
                  for (const part of message.parts) {
                    // Look for step-finish parts that contain usage data
                    if (part.type === 'step-finish' && part.tokens && part.cost) {
                      usageData.totalCostUsd += part.cost;
                      usageData.totalInputTokens += part.tokens.input || 0;
                      usageData.totalOutputTokens += part.tokens.output || 0;
                      usageData.totalCachedTokens += part.tokens.cache?.read || 0;
                      usageData.totalRequests += 1;

                      // Detect stage (probe/exec) from context
                      const stage = detectStageFromPart(part, message);
                      
                      if (!usageData.stages[stage]) {
                        usageData.stages[stage] = {
                          cost: 0,
                          inputTokens: 0,
                          outputTokens: 0,
                          requests: 0,
                          models: {}
                        };
                      }
                      
                      usageData.stages[stage].cost += part.cost;
                      usageData.stages[stage].inputTokens += part.tokens.input || 0;
                      usageData.stages[stage].outputTokens += part.tokens.output || 0;
                      usageData.stages[stage].requests += 1;

                      // Track model usage (if available in metadata)
                      const modelId = part.metadata?.model || 'unknown';
                      
                      if (!usageData.models[modelId]) {
                        usageData.models[modelId] = {
                          cost: 0,
                          tokens: 0,
                          requests: 0
                        };
                      }
                      
                      usageData.models[modelId].cost += part.cost;
                      usageData.models[modelId].tokens += (part.tokens.input || 0) + (part.tokens.output || 0);
                      usageData.models[modelId].requests += 1;

                      // Also track in stage models
                      if (!usageData.stages[stage].models[modelId]) {
                        usageData.stages[stage].models[modelId] = {
                          cost: 0,
                          tokens: 0,
                          requests: 0
                        };
                      }
                      
                      usageData.stages[stage].models[modelId].cost += part.cost;
                      usageData.stages[stage].models[modelId].tokens += (part.tokens.input || 0) + (part.tokens.output || 0);
                      usageData.stages[stage].models[modelId].requests += 1;
                    }
                  }
                }
              }
            }

            return c.json(usageData)
          } catch (error) {
            log.error("Failed to get session usage", { sessionId, error: error.message })
            return c.json({ error: "Failed to get session usage" }, 500)
          }
        },
      )
      .post(
        "/session/:id/notify-usage",
        describeRoute({
          description: "Notify backend of usage data for real-time cost tracking",
          operationId: "session.notifyUsage",
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: resolver(
                  z.object({
                    backendUrl: z.string(),
                    usageData: z.object({
                      stage: z.string(),
                      modelName: z.string(),
                      inputTokens: z.number(),
                      outputTokens: z.number(),
                      cachedTokens: z.number().optional(),
                      cost: z.number(),
                      duration: z.number().optional()
                    })
                  })
                ),
              },
            },
          },
          responses: {
            200: {
              description: "Usage notification sent successfully",
              content: {
                "application/json": {
                  schema: resolver(z.object({
                    success: z.boolean(),
                    message: z.string()
                  })),
                },
              },
            },
            400: ERRORS[400],
          },
        }),
        async (c) => {
          const sessionId = c.req.param("id")
          const { backendUrl, usageData } = await c.req.json()
          
          try {
            // Prepare comprehensive usage data for backend
            const backendPayload = {
              session_id: sessionId,
              stage: usageData.stage,
              model_name: usageData.modelName,
              model_provider: 'openrouter',
              input_tokens: usageData.inputTokens,
              output_tokens: usageData.outputTokens,
              cached_tokens: usageData.cachedTokens || 0,
              cost_usd: usageData.cost,
              requests: 1,
              duration_ms: usageData.duration || null,
              timestamp: new Date().toISOString()
            };

            // Send to Screener37 backend
            const response = await fetch(`${backendUrl}/api/v1/internal/usage`, {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'X-Internal-Request': 'opencode-usage-tracking'
              },
              body: JSON.stringify(backendPayload)
            });

            if (!response.ok) {
              throw new Error(`Backend responded with ${response.status}: ${response.statusText}`);
            }

            return c.json({
              success: true,
              message: "Usage data sent to backend successfully"
            });

          } catch (error) {
            log.error("Failed to notify backend of usage", { sessionId, error: error.message })
            return c.json({
              success: false,
              message: `Failed to notify backend: ${error.message}`
            }, 500);
          }
        },
      )
      .post(
        "/session/:id/switch-agent",
        describeRoute({
          description: "Switch agent for a session",
          operationId: "session.switchAgent",
          responses: {
            200: {
              description: "Agent switched successfully",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      success: z.boolean(),
                      currentAgent: z.string(),
                      previousAgent: z.string().optional(),
                      message: z.string().optional(),
                    })
                  ),
                },
              },
            },
            404: ERRORS[400],
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        zValidator(
          "json",
          z.object({
            agent: z.string().openapi({ description: "Target agent name (e.g., 'probe', 'exec')" }),
            message: z.string().optional().openapi({ description: "Optional message to send with the switch" }),
          }),
        ),
        async (c) => {
          const sessionId = c.req.valid("param").id
          const { agent, message } = c.req.valid("json")
          
          const session = await Session.get(sessionId)
          if (!session) {
            return c.json({ error: "Session not found" }, 404)
          }

          const previousAgent = session.context?.currentAgent || 'probe'
          
          // Update the session context
          await Session.update(sessionId, (draft) => {
            draft.context = {
              ...draft.context,
              currentAgent: agent,
              agentSwitchCount: (draft.context?.agentSwitchCount || 0) + 1,
              lastAgentSwitch: new Date().toISOString(),
            }
          })

          // Log agent switch event
          log.info("Agent switched", {
            sessionId,
            previousAgent,
            currentAgent: agent,
            message: message ? "with message" : "without message"
          })

          // Emit agent switch event
          Bus.emit(Server.Event.AgentSwitched, {
            sessionId,
            previousAgent,
            currentAgent: agent,
            timestamp: new Date().toISOString(),
          })

          return c.json({
            success: true,
            currentAgent: agent,
            previousAgent,
            message: message ? "Agent switched with message" : "Agent switched successfully",
          })
        },
      )
      .post(
        "/session/:id/message-with-agent",
        describeRoute({
          description: "Send message with optional agent switching",
          operationId: "session.messageWithAgent",
          responses: {
            200: {
              description: "Message sent with agent handling",
              content: {
                "application/json": {
                  schema: resolver(
                    z.object({
                      message: z.any(),
                      agentSwitched: z.boolean(),
                      currentAgent: z.string(),
                      switchSignalDetected: z.boolean().optional(),
                    })
                  ),
                },
              },
            },
            404: ERRORS[400],
          },
        }),
        zValidator(
          "param",
          z.object({
            id: z.string().openapi({ description: "Session ID" }),
          }),
        ),
        zValidator(
          "json",
          z.object({
            message: z.string().openapi({ description: "Message content" }),
            agent: z.string().optional().openapi({ description: "Force specific agent" }),
            autoSwitch: z.boolean().default(true).openapi({ description: "Enable automatic agent switching based on signals" }),
          }),
        ),
        async (c) => {
          const sessionId = c.req.valid("param").id
          const { message, agent, autoSwitch } = c.req.valid("json")
          
          const session = await Session.get(sessionId)
          if (!session) {
            return c.json({ error: "Session not found" }, 404)
          }

          let currentAgent = session.context?.currentAgent || 'probe'
          let agentSwitched = false
          let switchSignalDetected = false

          // Detect switch signal in message (from interface.py pattern)
          const switchSignalRegex = /XÆM-37.*(Switch|switch).*/
          if (autoSwitch && switchSignalRegex.test(message)) {
            switchSignalDetected = true
            // Switch from probe to exec when signal detected
            if (currentAgent === 'probe') {
              currentAgent = 'exec'
              agentSwitched = true
              
              // Update session context
              await Session.update(sessionId, (draft) => {
                draft.context = {
                  ...draft.context,
                  currentAgent,
                  agentSwitchCount: (draft.context?.agentSwitchCount || 0) + 1,
                  lastAgentSwitch: new Date().toISOString(),
                }
              })

              // Emit agent switch event
              Bus.emit(Server.Event.AgentSwitched, {
                sessionId,
                previousAgent: 'probe',
                currentAgent: 'exec',
                trigger: 'auto_signal',
                timestamp: new Date().toISOString(),
              })
            }
          }

          // Force agent if specified
          if (agent && agent !== currentAgent) {
            const previousAgent = currentAgent
            currentAgent = agent
            agentSwitched = true
            
            await Session.update(sessionId, (draft) => {
              draft.context = {
                ...draft.context,
                currentAgent,
                agentSwitchCount: (draft.context?.agentSwitchCount || 0) + 1,
                lastAgentSwitch: new Date().toISOString(),
              }
            })

            Bus.emit(Server.Event.AgentSwitched, {
              sessionId,
              previousAgent,
              currentAgent,
              trigger: 'manual',
              timestamp: new Date().toISOString(),
            })
          }

          // Emit status based on current agent
          const agentStatusMessage = currentAgent === 'probe' ? 'Planning research approach' : 'Executing analysis'
          emitAgentStatus(sessionId, currentAgent, currentAgent === 'probe' ? 'planning' : 'executing', agentStatusMessage)
          emitMessageProgress(sessionId, undefined, 'processing', 10)

          // Send message using the determined agent
          const chatInput = {
            sessionID: sessionId,
            message,
            agent: currentAgent,
          }

          try {
            const response = await Session.chat(chatInput)
            
            // Emit completion status
            emitAgentStatus(sessionId, currentAgent, 'completed', 'Analysis completed')
            emitMessageProgress(sessionId, undefined, 'completed', 100)

            return c.json({
              message: response,
              agentSwitched,
              currentAgent,
              switchSignalDetected,
            })
          } catch (error) {
            // Emit error status
            emitAgentStatus(sessionId, currentAgent, 'error', 'Error occurred during processing')
            emitMessageProgress(sessionId, undefined, 'error', undefined, { error: error.message })
            
            throw error
          }
        },
      )
      .post(
        "/tui/append-prompt",
        describeRoute({
          description: "Append prompt to the TUI",
          operationId: "tui.appendPrompt",
          responses: {
            200: {
              description: "Prompt processed successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "json",
          z.object({
            text: z.string(),
          }),
        ),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/open-help",
        describeRoute({
          description: "Open the help dialog",
          operationId: "tui.openHelp",
          responses: {
            200: {
              description: "Help dialog opened successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/open-sessions",
        describeRoute({
          description: "Open the session dialog",
          operationId: "tui.openSessions",
          responses: {
            200: {
              description: "Session dialog opened successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/open-themes",
        describeRoute({
          description: "Open the theme dialog",
          operationId: "tui.openThemes",
          responses: {
            200: {
              description: "Theme dialog opened successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/open-models",
        describeRoute({
          description: "Open the model dialog",
          operationId: "tui.openModels",
          responses: {
            200: {
              description: "Model dialog opened successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/submit-prompt",
        describeRoute({
          description: "Submit the prompt",
          operationId: "tui.submitPrompt",
          responses: {
            200: {
              description: "Prompt submitted successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/clear-prompt",
        describeRoute({
          description: "Clear the prompt",
          operationId: "tui.clearPrompt",
          responses: {
            200: {
              description: "Prompt cleared successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        async (c) => c.json(await callTui(c)),
      )
      .post(
        "/tui/execute-command",
        describeRoute({
          description: "Execute a TUI command (e.g. switch_agent)",
          operationId: "tui.executeCommand",
          responses: {
            200: {
              description: "Command executed successfully",
              content: {
                "application/json": {
                  schema: resolver(z.boolean()),
                },
              },
            },
          },
        }),
        zValidator(
          "json",
          z.object({
            command: z.string(),
          }),
        ),
        async (c) => c.json(await callTui(c)),
      )
      .route("/tui/control", TuiRoute)

    return result
  })

  // Helper function to detect stage from message part and context
  function detectStageFromPart(part: any, message: any): string {
    // Check for explicit agent metadata
    if (part.metadata?.agent === 'exec' || message.metadata?.agent === 'exec') {
      return 'exec';
    }
    if (part.metadata?.agent === 'probe' || message.metadata?.agent === 'probe') {
      return 'probe';
    }

    // Check for model patterns to infer stage
    const modelId = part.metadata?.model || '';
    if (modelId.includes('deepseek') || modelId.includes('kimi')) {
      return 'exec';
    }
    if (modelId.includes('claude') || modelId.includes('sonnet')) {
      return 'probe';
    }

    // Check message content patterns
    const content = message.content || '';
    if (content.includes('executing') || content.includes('running') || content.includes('tool')) {
      return 'exec';
    }

    // Default to probe (most common starting stage)
    return 'probe';
  }

  export async function openapi() {
    const a = app()
    const result = await generateSpecs(a, {
      documentation: {
        info: {
          title: "opencode",
          version: "1.0.0",
          description: "opencode api",
        },
        openapi: "3.0.0",
      },
    })
    return result
  }

  export function listen(opts: { port: number; hostname: string }) {
    const server = Bun.serve({
      port: opts.port,
      hostname: opts.hostname,
      idleTimeout: 0,
      fetch: app().fetch,
    })
    return server
  }
}
