import path from "path"
import { Decimal } from "decimal.js"
import { z, ZodSchema } from "zod"
import {
  generateText,
  LoadAPIKeyError,
  streamText,
  tool,
  wrapLanguageModel,
  type Tool as AITool,
  type LanguageModelUsage,
  type ProviderMetadata,
  type ModelMessage,
  stepCountIs,
  type StreamTextResult,
} from "ai"

import PROMPT_INITIALIZE from "../session/prompt/initialize.txt"
import PROMPT_PLAN from "../session/prompt/plan.txt"

import { App } from "../app/app"
import { Auth } from "../auth"
import { Bus } from "../bus"
import { Config } from "../config/config"
import { Flag } from "../flag/flag"
import { Identifier } from "../id/id"
import { Installation } from "../installation"
import { MCP } from "../mcp"
import { Provider } from "../provider/provider"
import { ProviderTransform } from "../provider/transform"
import type { ModelsDev } from "../provider/models"
import { Share } from "../share/share"
import { Snapshot } from "../snapshot"
import { Storage } from "../storage/storage"
import { Log } from "../util/log"
import { NamedError } from "../util/error"
import { SystemPrompt } from "./system"
import { FileTime } from "../file/time"
import { MessageV2 } from "./message-v2"
import { LSP } from "../lsp"
import { ReadTool } from "../tool/read"
import { mergeDeep, pipe, splitWhen } from "remeda"
import { ToolRegistry } from "../tool/registry"
import { Plugin } from "../plugin"
import { Agent } from "../agent/agent"
import { Server } from "../server/server"

export namespace Session {
  const log = Log.create({ service: "session" })

  const OUTPUT_TOKEN_MAX = 32_000

  const parentSessionTitlePrefix = "New session - "
  const childSessionTitlePrefix = "Child session - "
  
  // Cache for OpenRouter generation IDs (session -> generation_id)
  const generationIdCache = new Map<string, string>()

// Store generation ID in database ledger for tracking and verification
async function storeGenerationIdInLedger(
  generationId: string, 
  sessionID: string, 
  stage: string, 
  modelName: string, 
  modelProvider: string,
  finishReason?: string
) {
  try {
    const backendUrl = process.env.SCREENER37_BACKEND_URL || 'http://localhost:8000';
    
    const response = await fetch(`${backendUrl}/api/v1/internal/generation-ledger`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Internal-Request': 'opencode-generation-ledger'
      },
      body: JSON.stringify({
        generation_id: generationId,
        opencode_session_id: sessionID,
        stage: stage,
        model_name: modelName,
        model_provider: modelProvider,
        finish_reason: finishReason
      }),
      signal: AbortSignal.timeout(5000)
    });
    
    if (response.ok) {
      log.debug("Generation ID stored in ledger", { generationId, sessionID, stage });
    } else {
      log.warn("Failed to store generation ID in ledger", { 
        generationId, 
        sessionID, 
        status: response.status,
        statusText: response.statusText 
      });
    }
  } catch (error) {
    log.error("Error storing generation ID in ledger", { 
      generationId, 
      sessionID, 
      error: error.message 
    });
  }
}
  
  // Simple cost tracking logger
  function logCostStep(step: string, sessionID: string, data: any) {
    log.info(`💰 COST-STEP: ${step}`, { sessionID, ...data });
  }

  function createDefaultTitle(isChild = false) {
    return (isChild ? childSessionTitlePrefix : parentSessionTitlePrefix) + new Date().toISOString()
  }

  function isDefaultTitle(title: string) {
    return title.startsWith(parentSessionTitlePrefix)
  }

  export const Info = z
    .object({
      id: Identifier.schema("session"),
      parentID: Identifier.schema("session").optional(),
      share: z
        .object({
          url: z.string(),
        })
        .optional(),
      title: z.string(),
      version: z.string(),
      time: z.object({
        created: z.number(),
        updated: z.number(),
      }),
      revert: z
        .object({
          messageID: z.string(),
          partID: z.string().optional(),
          snapshot: z.string().optional(),
          diff: z.string().optional(),
        })
        .optional(),
      context: z
        .object({
          currentAgent: z.string().optional(),
          agentSwitchCount: z.number().optional(),
          lastAgentSwitch: z.string().optional(),
          sessionTokens: z.number().optional(),
        })
        .optional(),
    })
    .openapi({
      ref: "Session",
    })
  export type Info = z.output<typeof Info>

  export const ShareInfo = z
    .object({
      secret: z.string(),
      url: z.string(),
    })
    .openapi({
      ref: "SessionShare",
    })
  export type ShareInfo = z.output<typeof ShareInfo>

  export const Event = {
    Updated: Bus.event(
      "session.updated",
      z.object({
        info: Info,
      }),
    ),
    Deleted: Bus.event(
      "session.deleted",
      z.object({
        info: Info,
      }),
    ),
    Idle: Bus.event(
      "session.idle",
      z.object({
        sessionID: z.string(),
      }),
    ),
    Error: Bus.event(
      "session.error",
      z.object({
        sessionID: z.string().optional(),
        error: MessageV2.Assistant.shape.error,
      }),
    ),
    MessageTextStart: Bus.event(
      "message.text.start",
      z.object({
        sessionId: z.string(),
        messageId: z.string(),
        partId: z.string(),
        timestamp: z.string(),
      }),
    ),
    MessageTextDelta: Bus.event(
      "message.text.delta",
      z.object({
        sessionId: z.string(),
        messageId: z.string(),
        partId: z.string(),
        content: z.string(),
        fullText: z.string(),
        timestamp: z.string(),
      }),
    ),
    MessageTextEnd: Bus.event(
      "message.text.end",
      z.object({
        sessionId: z.string(),
        messageId: z.string(),
        partId: z.string(),
        finalText: z.string(),
        timestamp: z.string(),
      }),
    ),
    MessageFinished: Bus.event(
      "message.finished",
      z.object({
        sessionId: z.string(),
        messageId: z.string(),
        timestamp: z.string(),
      }),
    ),
  }

  const state = App.state(
    "session",
    () => {
      const sessions = new Map<string, Info>()
      const messages = new Map<string, MessageV2.Info[]>()
      const pending = new Map<string, AbortController>()
      const autoCompacting = new Map<string, boolean>()
      const queued = new Map<
        string,
        {
          input: ChatInput
          message: MessageV2.User
          parts: MessageV2.Part[]
          processed: boolean
          callback: (input: { info: MessageV2.Assistant; parts: MessageV2.Part[] }) => void
        }[]
      >()

      return {
        sessions,
        messages,
        pending,
        autoCompacting,
        queued,
      }
    },
    async (state) => {
      for (const [_, controller] of state.pending) {
        controller.abort()
      }
    },
  )

  export async function create(parentID?: string) {
    const result: Info = {
      id: Identifier.descending("session"),
      version: Installation.VERSION,
      parentID,
      title: createDefaultTitle(!!parentID),
      time: {
        created: Date.now(),
        updated: Date.now(),
      },
      context: {
        currentAgent: 'probe', // Default to probe agent
        agentSwitchCount: 0,
        sessionTokens: 0,
      },
    }
    log.info("created", result)
    state().sessions.set(result.id, result)
    await Storage.writeJSON("session/info/" + result.id, result)
    const cfg = await Config.get()
    if (!result.parentID && (Flag.OPENCODE_AUTO_SHARE || cfg.share === "auto"))
      share(result.id)
        .then((share) => {
          update(result.id, (draft) => {
            draft.share = share
          })
        })
        .catch(() => {
          // Silently ignore sharing errors during session creation
        })
    Bus.publish(Event.Updated, {
      info: result,
    })
    return result
  }

  export async function get(id: string) {
    const result = state().sessions.get(id)
    if (result) {
      return result
    }
    const read = await Storage.readJSON<Info>("session/info/" + id)
    state().sessions.set(id, read)
    return read as Info
  }

  export async function getShare(id: string) {
    return Storage.readJSON<ShareInfo>("session/share/" + id)
  }

  export async function share(id: string) {
    const cfg = await Config.get()
    if (cfg.share === "disabled") {
      throw new Error("Sharing is disabled in configuration")
    }

    const session = await get(id)
    if (session.share) return session.share
    const share = await Share.create(id)
    await update(id, (draft) => {
      draft.share = {
        url: share.url,
      }
    })
    await Storage.writeJSON<ShareInfo>("session/share/" + id, share)
    await Share.sync("session/info/" + id, session)
    for (const msg of await messages(id)) {
      await Share.sync("session/message/" + id + "/" + msg.info.id, msg.info)
      for (const part of msg.parts) {
        await Share.sync("session/part/" + id + "/" + msg.info.id + "/" + part.id, part)
      }
    }
    return share
  }

  export async function unshare(id: string) {
    const share = await getShare(id)
    if (!share) return
    await Storage.remove("session/share/" + id)
    await update(id, (draft) => {
      draft.share = undefined
    })
    await Share.remove(id, share.secret)
  }

  export async function update(id: string, editor: (session: Info) => void) {
    const { sessions } = state()
    const session = await get(id)
    if (!session) return
    editor(session)
    session.time.updated = Date.now()
    sessions.set(id, session)
    await Storage.writeJSON("session/info/" + id, session)
    Bus.publish(Event.Updated, {
      info: session,
    })
    return session
  }

  export async function messages(sessionID: string) {
    const result = [] as {
      info: MessageV2.Info
      parts: MessageV2.Part[]
    }[]
    for (const p of await Storage.list("session/message/" + sessionID)) {
      const read = await Storage.readJSON<MessageV2.Info>(p)
      result.push({
        info: read,
        parts: await getParts(sessionID, read.id),
      })
    }
    result.sort((a, b) => (a.info.id > b.info.id ? 1 : -1))
    return result
  }

  export async function getMessage(sessionID: string, messageID: string) {
    return {
      info: await Storage.readJSON<MessageV2.Info>("session/message/" + sessionID + "/" + messageID),
      parts: await getParts(sessionID, messageID),
    }
  }

  export async function getParts(sessionID: string, messageID: string) {
    const result = [] as MessageV2.Part[]
    for (const item of await Storage.list("session/part/" + sessionID + "/" + messageID)) {
      const read = await Storage.readJSON<MessageV2.Part>(item)
      result.push(read)
    }
    result.sort((a, b) => (a.id > b.id ? 1 : -1))
    return result
  }

  export async function* list() {
    for (const item of await Storage.list("session/info")) {
      const sessionID = path.basename(item, ".json")
      yield get(sessionID)
    }
  }

  export async function children(parentID: string) {
    const result = [] as Session.Info[]
    for (const item of await Storage.list("session/info")) {
      const sessionID = path.basename(item, ".json")
      const session = await get(sessionID)
      if (session.parentID !== parentID) continue
      result.push(session)
    }
    return result
  }

  export function abort(sessionID: string) {
    const controller = state().pending.get(sessionID)
    if (!controller) return false
    log.info("aborting", {
      sessionID,
    })
    controller.abort()
    state().pending.delete(sessionID)
    return true
  }

  export async function remove(sessionID: string, emitEvent = true) {
    try {
      abort(sessionID)
      const session = await get(sessionID)
      for (const child of await children(sessionID)) {
        await remove(child.id, false)
      }
      await unshare(sessionID).catch(() => { })
      await Storage.remove(`session/info/${sessionID}`).catch(() => { })
      await Storage.removeDir(`session/message/${sessionID}/`).catch(() => { })
      state().sessions.delete(sessionID)
      state().messages.delete(sessionID)
      if (emitEvent) {
        Bus.publish(Event.Deleted, {
          info: session,
        })
      }
    } catch (e) {
      log.error(e)
    }
  }

  async function updateMessage(msg: MessageV2.Info) {
    await Storage.writeJSON("session/message/" + msg.sessionID + "/" + msg.id, msg)
    Bus.publish(MessageV2.Event.Updated, {
      info: msg,
    })
  }

  async function updatePart(part: MessageV2.Part) {
    await Storage.writeJSON(["session", "part", part.sessionID, part.messageID, part.id].join("/"), part)
    Bus.publish(MessageV2.Event.PartUpdated, {
      part,
    })
    return part
  }

  export const ChatInput = z.object({
    sessionID: Identifier.schema("session"),
    messageID: Identifier.schema("message").optional(),
    providerID: z.string(),
    modelID: z.string(),
    agent: z.string().optional(),
    system: z.string().optional(),
    tools: z.record(z.boolean()).optional(),
    parts: z.array(
      z.discriminatedUnion("type", [
        MessageV2.TextPart.omit({
          messageID: true,
          sessionID: true,
        })
          .partial({
            id: true,
          })
          .openapi({
            ref: "TextPartInput",
          }),
        MessageV2.FilePart.omit({
          messageID: true,
          sessionID: true,
        })
          .partial({
            id: true,
          })
          .openapi({
            ref: "FilePartInput",
          }),
        MessageV2.AgentPart.omit({
          messageID: true,
          sessionID: true,
        })
          .partial({
            id: true,
          })
          .openapi({
            ref: "AgentPartInput",
          }),
      ]),
    ),
  })
  export type ChatInput = z.infer<typeof ChatInput>

  export async function chat(
    input: z.infer<typeof ChatInput>,
  ): Promise<{ info: MessageV2.Assistant; parts: MessageV2.Part[] }> {
    const l = log.clone().tag("session", input.sessionID)
    l.info("chatting")

    const inputAgent = input.agent ?? "build"

    // Process revert cleanup first, before creating new messages
    const session = await get(input.sessionID)
    if (session.revert) {
      let msgs = await messages(input.sessionID)
      const messageID = session.revert.messageID
      const [preserve, remove] = splitWhen(msgs, (x) => x.info.id === messageID)
      msgs = preserve
      for (const msg of remove) {
        await Storage.remove(`session/message/${input.sessionID}/${msg.info.id}`)
        await Bus.publish(MessageV2.Event.Removed, { sessionID: input.sessionID, messageID: msg.info.id })
      }
      const last = preserve.at(-1)
      if (session.revert.partID && last) {
        const partID = session.revert.partID
        const [preserveParts, removeParts] = splitWhen(last.parts, (x) => x.id === partID)
        last.parts = preserveParts
        for (const part of removeParts) {
          await Storage.remove(`session/part/${input.sessionID}/${last.info.id}/${part.id}`)
          await Bus.publish(MessageV2.Event.PartRemoved, {
            sessionID: input.sessionID,
            messageID: last.info.id,
            partID: part.id,
          })
        }
      }
      await update(input.sessionID, (draft) => {
        draft.revert = undefined
      })
    }
    const userMsg: MessageV2.Info = {
      id: input.messageID ?? Identifier.ascending("message"),
      role: "user",
      sessionID: input.sessionID,
      time: {
        created: Date.now(),
      },
    }

    const app = App.info()
    const userParts = await Promise.all(
      input.parts.map(async (part): Promise<MessageV2.Part[]> => {
        if (part.type === "file") {
          const url = new URL(part.url)
          switch (url.protocol) {
            case "data:":
              if (part.mime === "text/plain") {
                return [
                  {
                    id: Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                    type: "text",
                    synthetic: true,
                    text: `Called the Read tool with the following input: ${JSON.stringify({ filePath: part.filename })}`,
                  },
                  {
                    id: Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                    type: "text",
                    synthetic: true,
                    text: Buffer.from(part.url, "base64url").toString(),
                  },
                  {
                    ...part,
                    id: part.id ?? Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                  },
                ]
              }
              break
            case "file:":
              // have to normalize, symbol search returns absolute paths
              // Decode the pathname since URL constructor doesn't automatically decode it
              const filePath = decodeURIComponent(url.pathname)

              if (part.mime === "text/plain") {
                let offset: number | undefined = undefined
                let limit: number | undefined = undefined
                const range = {
                  start: url.searchParams.get("start"),
                  end: url.searchParams.get("end"),
                }
                if (range.start != null) {
                  const filePath = part.url.split("?")[0]
                  let start = parseInt(range.start)
                  let end = range.end ? parseInt(range.end) : undefined
                  // some LSP servers (eg, gopls) don't give full range in
                  // workspace/symbol searches, so we'll try to find the
                  // symbol in the document to get the full range
                  if (start === end) {
                    const symbols = await LSP.documentSymbol(filePath)
                    for (const symbol of symbols) {
                      let range: LSP.Range | undefined
                      if ("range" in symbol) {
                        range = symbol.range
                      } else if ("location" in symbol) {
                        range = symbol.location.range
                      }
                      if (range?.start?.line && range?.start?.line === start) {
                        start = range.start.line
                        end = range?.end?.line ?? start
                        break
                      }
                    }
                    offset = Math.max(start - 2, 0)
                    if (end) {
                      limit = end - offset + 2
                    }
                  }
                }
                const args = { filePath, offset, limit }
                const result = await ReadTool.init().then((t) =>
                  t.execute(args, {
                    sessionID: input.sessionID,
                    abort: new AbortController().signal,
                    messageID: userMsg.id,
                    metadata: async () => { },
                  }),
                )
                return [
                  {
                    id: Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                    type: "text",
                    synthetic: true,
                    text: `Called the Read tool with the following input: ${JSON.stringify(args)}`,
                  },
                  {
                    id: Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                    type: "text",
                    synthetic: true,
                    text: result.output,
                  },
                  {
                    ...part,
                    id: part.id ?? Identifier.ascending("part"),
                    messageID: userMsg.id,
                    sessionID: input.sessionID,
                  },
                ]
              }

              let file = Bun.file(filePath)
              FileTime.read(input.sessionID, filePath)
              return [
                {
                  id: Identifier.ascending("part"),
                  messageID: userMsg.id,
                  sessionID: input.sessionID,
                  type: "text",
                  text: `Called the Read tool with the following input: {\"filePath\":\"${filePath}\"}`,
                  synthetic: true,
                },
                {
                  id: part.id ?? Identifier.ascending("part"),
                  messageID: userMsg.id,
                  sessionID: input.sessionID,
                  type: "file",
                  url: `data:${part.mime};base64,` + Buffer.from(await file.bytes()).toString("base64"),
                  mime: part.mime,
                  filename: part.filename!,
                  source: part.source,
                },
              ]
          }
        }

        if (part.type === "agent") {
          return [
            {
              id: Identifier.ascending("part"),
              ...part,
              messageID: userMsg.id,
              sessionID: input.sessionID,
            },
            {
              id: Identifier.ascending("part"),
              messageID: userMsg.id,
              sessionID: input.sessionID,
              type: "text",
              synthetic: true,
              text:
                "Use the above message and context to generate a prompt and call the task tool with subagent: " +
                part.name,
            },
          ]
        }

        return [
          {
            id: Identifier.ascending("part"),
            ...part,
            messageID: userMsg.id,
            sessionID: input.sessionID,
          },
        ]
      }),
    ).then((x) => x.flat())
    if (inputAgent === "plan")
      userParts.push({
        id: Identifier.ascending("part"),
        messageID: userMsg.id,
        sessionID: input.sessionID,
        type: "text",
        text: PROMPT_PLAN,
        synthetic: true,
      })
    await Plugin.trigger(
      "chat.message",
      {},
      {
        message: userMsg,
        parts: userParts,
      },
    )
    await updateMessage(userMsg)
    for (const part of userParts) {
      await updatePart(part)
    }

    // mark session as updated
    // used for session list sorting (indicates when session was most recently interacted with)
    await update(input.sessionID, (_draft) => { })

    if (isLocked(input.sessionID)) {
      return new Promise((resolve) => {
        const queue = state().queued.get(input.sessionID) ?? []
        queue.push({
          input: input,
          message: userMsg,
          parts: userParts,
          processed: false,
          callback: resolve,
        })
        state().queued.set(input.sessionID, queue)
      })
    }

    const model = await Provider.getModel(input.providerID, input.modelID)
    let msgs = await messages(input.sessionID)

    const previous = msgs.filter((x) => x.info.role === "assistant").at(-1)?.info as MessageV2.Assistant
    const outputLimit = Math.min(model.info.limit.output, OUTPUT_TOKEN_MAX) || OUTPUT_TOKEN_MAX

    // auto summarize if too long
    if (previous && previous.tokens) {
      const tokens =
        previous.tokens.input + previous.tokens.cache.read + previous.tokens.cache.write + previous.tokens.output
      if (model.info.limit.context && tokens > Math.max((model.info.limit.context - outputLimit) * 0.9, 0)) {
        state().autoCompacting.set(input.sessionID, true)

        await summarize({
          sessionID: input.sessionID,
          providerID: input.providerID,
          modelID: input.modelID,
        })
        return chat(input)
      }
    }
    using abort = lock(input.sessionID)

    const lastSummary = msgs.findLast((msg) => msg.info.role === "assistant" && msg.info.summary === true)
    if (lastSummary) msgs = msgs.filter((msg) => msg.info.id >= lastSummary.info.id)

    if (msgs.length === 1 && !session.parentID && isDefaultTitle(session.title)) {
      const small = (await Provider.getSmallModel(input.providerID)) ?? model
      generateText({
        maxOutputTokens: small.info.reasoning ? 1024 : 20,
        providerOptions: {
          [input.providerID]: small.info.options,
        },
        messages: [
          ...SystemPrompt.title(input.providerID).map(
            (x): ModelMessage => ({
              role: "system",
              content: x,
            }),
          ),
          ...MessageV2.toModelMessage([
            {
              info: {
                id: Identifier.ascending("message"),
                role: "user",
                sessionID: input.sessionID,
                time: {
                  created: Date.now(),
                },
              },
              parts: userParts,
            },
          ]),
        ],
        model: small.language,
      })
        .then(async (result) => {
          if (result.text) {
            // Extract OpenRouter completion ID for title generation (same as main chat)
            const isOpenRouter = input.providerID?.toLowerCase() === 'openrouter' || 
                               input.modelID?.includes('openrouter');
            
            if (isOpenRouter && result.response?.id) {
              logCostStep("TITLE-COMPLETION-ID", input.sessionID, { 
                completionId: result.response.id,
                stage: "title_generation"
              });
              
              // Store generation_id in cache for title generation cost tracking
              generationIdCache.set(input.sessionID + "_title", result.response.id);
              
              // Store in database ledger for tracking and verification
              storeGenerationIdInLedger(
                result.response.id,
                input.sessionID,
                "title",
                input.modelID || small.info.id || "unknown",
                "openrouter",
                "stop"
              ).catch(e => log.warn("Failed to store title generation ID in ledger", e));
            }
            
            // Track title generation cost
            try {
              const titleUsage = await getUsage(small.info, result.usage, result.experimental_providerMetadata, input.sessionID + "_title")
              log.info("Title generation cost tracked", { 
                sessionID: input.sessionID, 
                cost: titleUsage.cost, 
                tokens: titleUsage.tokens,
                model: small.info.id,
                // Check if we got actual cost from OpenRouter or used calculated
                actualCost: (titleUsage.cost > 0 && generationIdCache.has(input.sessionID + "_title")) ? "OpenRouter API" : "calculated"
              })
              
              // Send title generation usage to backend using the correct function
              await notifyBackendUsage(input.sessionID, titleUsage.tokens, titleUsage.cost, small.info, "title_generation")
              
            } catch (error) {
              log.warn("Failed to track title generation cost", { error: error.message || error, sessionID: input.sessionID })
            }
            
            return Session.update(input.sessionID, (draft) => {
              const cleaned = result.text.replace(/<think>[\s\S]*?<\/think>\s*/g, "")
              const title = cleaned.length > 100 ? cleaned.substring(0, 97) + "..." : cleaned
              draft.title = title.trim()
            })
          }
        })
        .catch(() => { })
    }

    const agent = await Agent.get(inputAgent)
    let system = SystemPrompt.header(input.providerID)
    system.push(
      ...(() => {
        if (input.system) return [input.system]
        if (agent.prompt) return [agent.prompt]
        return SystemPrompt.provider(input.modelID)
      })(),
    )
    system.push(...(await SystemPrompt.environment()))
    system.push(...(await SystemPrompt.custom()))
    // max 2 system prompt messages for caching purposes
    const [first, ...rest] = system
    system = [first, rest.join("\n")]

    const assistantMsg: MessageV2.Info = {
      id: Identifier.ascending("message"),
      role: "assistant",
      system,
      mode: inputAgent,
      path: {
        cwd: app.path.cwd,
        root: app.path.root,
      },
      cost: 0,
      tokens: {
        input: 0,
        output: 0,
        reasoning: 0,
        cache: { read: 0, write: 0 },
      },
      modelID: input.modelID,
      providerID: input.providerID,
      time: {
        created: Date.now(),
      },
      sessionID: input.sessionID,
    }
    await updateMessage(assistantMsg)
    const tools: Record<string, AITool> = {}

    const processor = createProcessor(assistantMsg, model.info)

    const enabledTools = pipe(
      agent.tools,
      mergeDeep(await ToolRegistry.enabled(input.providerID, input.modelID)),
      mergeDeep(input.tools ?? {}),
    )
    for (const item of await ToolRegistry.tools(input.providerID, input.modelID)) {
      if (enabledTools[item.id] === false) continue
      tools[item.id] = tool({
        id: item.id as any,
        description: item.description,
        inputSchema: item.parameters as ZodSchema,
        async execute(args, options) {
          await Plugin.trigger(
            "tool.execute.before",
            {
              tool: item.id,
              sessionID: input.sessionID,
              callID: options.toolCallId,
            },
            {
              args,
            },
          )
          const result = await item.execute(args, {
            sessionID: input.sessionID,
            abort: options.abortSignal!,
            messageID: assistantMsg.id,
            callID: options.toolCallId,
            metadata: async (val) => {
              const match = processor.partFromToolCall(options.toolCallId)
              if (match && match.state.status === "running") {
                await updatePart({
                  ...match,
                  state: {
                    title: val.title,
                    metadata: val.metadata,
                    status: "running",
                    input: args,
                    time: {
                      start: Date.now(),
                    },
                  },
                })
              }
            },
          })
          await Plugin.trigger(
            "tool.execute.after",
            {
              tool: item.id,
              sessionID: input.sessionID,
              callID: options.toolCallId,
            },
            result,
          )
          return result
        },
        toModelOutput(result) {
          return {
            type: "text",
            value: result.output,
          }
        },
      })
    }

    for (const [key, item] of Object.entries(await MCP.tools())) {
      if (enabledTools[key] === false) continue
      const execute = item.execute
      if (!execute) continue
      item.execute = async (args, opts) => {
        const result = await execute(args, opts)
        const output = result.content
          .filter((x: any) => x.type === "text")
          .map((x: any) => x.text)
          .join("\n\n")

        return {
          output,
        }
      }
      item.toModelOutput = (result) => {
        return {
          type: "text",
          value: result.output,
        }
      }
      tools[key] = item
    }

    const params = {
      temperature: model.info.temperature
        ? (agent.temperature ?? ProviderTransform.temperature(input.providerID, input.modelID))
        : undefined,
      topP: agent.topP ?? ProviderTransform.topP(input.providerID, input.modelID),
    }
    await Plugin.trigger(
      "chat.params",
      {
        model: model.info,
        provider: await Provider.getProvider(input.providerID),
        message: userMsg,
      },
      params,
    )

    // Pre-flight balance check
    const hasBalance = await checkUserBalance(input.sessionID, 0.10); // Estimate $0.10 for pre-flight check
    if (!hasBalance) {
      throw new NamedError("InsufficientBalance", "Insufficient wallet balance to continue. Please add funds to your account.");
    }

    const stream = streamText({
      onError(e) {
        log.error("streamText error", {
          error: e,
        })
      },
      onFinish(result) {
        // Extract OpenRouter completion ID from response and inject into metadata
        const isOpenRouter = input.providerID?.toLowerCase() === 'openrouter' || 
                           input.modelID?.includes('openrouter');
        
        // COMPREHENSIVE DEBUGGING for generation ID cache failures
        log.info("=== GENERATION ID CACHE DEBUG ===", {
          sessionID: input.sessionID,
          providerID: input.providerID,
          modelID: input.modelID,
          isOpenRouter: isOpenRouter,
          hasResponse: !!result.response,
          hasResponseId: !!result.response?.id,
          responseId: result.response?.id,
          finishReason: result.finishReason,
          responseKeys: result.response ? Object.keys(result.response) : 'no_response'
        });
        
        if (isOpenRouter && result.response?.id) {
          logCostStep("PROBE-COMPLETION-ID", input.sessionID, { 
            completionId: result.response.id, 
            stage: "probe"
          });
          
          // Store generation_id in cache for getUsage function
          generationIdCache.set(input.sessionID, result.response.id);
          
          // Store in database ledger for tracking and verification
          storeGenerationIdInLedger(
            result.response.id,
            input.sessionID,
            "probe",
            input.modelID || "unknown",
            "openrouter",
            result.finishReason
          ).catch(e => log.warn("Failed to store probe generation ID in ledger", e));
          
          log.info("✅ GENERATION ID CACHED", {
            sessionID: input.sessionID,
            generationId: result.response.id,
            cacheSize: generationIdCache.size
          });
          
          // Also inject into metadata (though this might not be passed through)
          if (!result.experimental_providerMetadata) {
            result.experimental_providerMetadata = {};
          }
          if (!result.experimental_providerMetadata.openrouter) {
            result.experimental_providerMetadata.openrouter = {};
          }
          result.experimental_providerMetadata.openrouter.generation_id = result.response.id;
        } else {
          log.warn("❌ GENERATION ID NOT CACHED", {
            sessionID: input.sessionID,
            reason: !isOpenRouter ? "not_openrouter" : !result.response?.id ? "no_response_id" : "unknown",
            isOpenRouter: isOpenRouter,
            hasResponseId: !!result.response?.id
          });
        }
        
        log.info("=== STREAMTEXT FINISH DEBUG ===", {
          sessionID: input.sessionID,
          usage: JSON.stringify(result.usage),
          response: JSON.stringify(result.response, null, 2),
          experimental_providerMetadata: JSON.stringify(result.experimental_providerMetadata, null, 2),
          finishReason: result.finishReason,
          text: result.text?.substring(0, 100) + "..."
        });
      },
      async prepareStep({ messages }) {
        const queue = (state().queued.get(input.sessionID) ?? []).filter((x) => !x.processed)
        if (queue.length) {
          for (const item of queue) {
            if (item.processed) continue
            messages.push(
              ...MessageV2.toModelMessage([
                {
                  info: item.message,
                  parts: item.parts,
                },
              ]),
            )
            item.processed = true
          }
          assistantMsg.time.completed = Date.now()
          await updateMessage(assistantMsg)
          Object.assign(assistantMsg, {
            id: Identifier.ascending("message"),
            role: "assistant",
            system,
            path: {
              cwd: app.path.cwd,
              root: app.path.root,
            },
            cost: 0,
            tokens: {
              input: 0,
              output: 0,
              reasoning: 0,
              cache: { read: 0, write: 0 },
            },
            modelID: input.modelID,
            providerID: input.providerID,
            mode: inputAgent,
            time: {
              created: Date.now(),
            },
            sessionID: input.sessionID,
          })
          await updateMessage(assistantMsg)
        }
        return {
          messages,
        }
      },
      //async experimental_repairToolCall(input) {
      //  return {
      //    ...input.toolCall,
      //    input: JSON.stringify({
      //      tool: input.toolCall.toolName,
      //      error: input.error.message,
      //    }),
      //    toolName: "invalid",
      //  }
      //},
      maxRetries: 3,
      activeTools: Object.keys(tools).filter((x) => x !== "invalid"),
      maxOutputTokens: model.info.id.startsWith("gpt-5") ? undefined : outputLimit,
      abortSignal: abort.signal,
      stopWhen: stepCountIs(1000),
      providerOptions: {
        [input.providerID]: model.info.options,
      },
      temperature: params.temperature,
      topP: params.topP,
      messages: [
        ...system.map(
          (x): ModelMessage => ({
            role: "system",
            content: x,
          }),
        ),
        ...MessageV2.toModelMessage(msgs),
      ],
      tools: model.info.tool_call === false ? undefined : tools,
      model: wrapLanguageModel({
        model: model.language,
        middleware: [
          {
            async transformParams(args) {
              if (args.type === "stream") {
                // @ts-expect-error
                args.params.prompt = ProviderTransform.message(args.params.prompt, input.providerID, input.modelID)
              }
              return args.params
            },
          },
        ],
      }),
    })
    const result = await processor.process(stream)
    const queued = state().queued.get(input.sessionID) ?? []
    const unprocessed = queued.find((x) => !x.processed)
    if (unprocessed) {
      unprocessed.processed = true
      return chat(unprocessed.input)
    }
    for (const item of queued) {
      item.callback(result)
    }
    state().queued.delete(input.sessionID)
    return result
  }

  function createProcessor(assistantMsg: MessageV2.Assistant, model: ModelsDev.Model) {
    const toolcalls: Record<string, MessageV2.ToolPart> = {}
    let snapshot: string | undefined
    return {
      partFromToolCall(toolCallID: string) {
        return toolcalls[toolCallID]
      },
      async process(stream: StreamTextResult<Record<string, AITool>, never>) {
        try {
          let currentText: MessageV2.TextPart | undefined

          for await (const value of stream.fullStream) {
            log.info("part", {
              type: value.type,
            })
            switch (value.type) {
              case "start":
                break

              case "tool-input-start":
                const part = await updatePart({
                  id: toolcalls[value.id]?.id ?? Identifier.ascending("part"),
                  messageID: assistantMsg.id,
                  sessionID: assistantMsg.sessionID,
                  type: "tool",
                  tool: value.toolName,
                  // Added by Akshay. Sanitize toolCallId to ensure it matches the required pattern ^[a-zA-Z0-9_-]+$
                  callID: value.id.replace(/[^a-zA-Z0-9_-]/g, '_'),
                  state: {
                    status: "pending",
                  },
                })
                toolcalls[value.id] = part as MessageV2.ToolPart
                break

              case "tool-input-delta":
                break

              case "tool-input-end":
                break

              case "tool-call": {
                const match = toolcalls[value.toolCallId]
                if (match) {
                  const part = await updatePart({
                    ...match,
                    tool: value.toolName,
                    state: {
                      status: "running",
                      input: value.input,
                      time: {
                        start: Date.now(),
                      },
                    },
                  })
                  toolcalls[value.toolCallId] = part as MessageV2.ToolPart
                  
                  // Emit tool start event for session event streaming (non-blocking)
                  Bus.publish(Server.Event.ToolUsage, {
                    sessionId: assistantMsg.sessionID,
                    agent: assistantMsg.agent || 'unknown',
                    tool: value.toolName,
                    action: 'start',
                    description: value.input?.description || `Running ${value.toolName}`,
                    data: value.input,
                    timestamp: new Date().toISOString(),
                  }).catch(e => log.warn('Bus publish failed', e))
                }
                break
              }
              case "tool-result": {
                const match = toolcalls[value.toolCallId]
                if (match && match.state.status === "running") {
                  await updatePart({
                    ...match,
                    state: {
                      status: "completed",
                      input: value.input,
                      output: value.output.output,
                      metadata: value.output.metadata,
                      title: value.output.title,
                      time: {
                        start: match.state.time.start,
                        end: Date.now(),
                      },
                    },
                  })
                  
                  // Emit tool completion event for session event streaming (non-blocking)
                  Bus.publish(Server.Event.ToolUsage, {
                    sessionId: assistantMsg.sessionID,
                    agent: assistantMsg.agent || 'unknown',
                    tool: match.tool,
                    action: 'complete',
                    description: value.output.metadata?.description || `Completed ${match.tool}`,
                    data: {
                      output: value.output.output,
                      title: value.output.title,
                      metadata: value.output.metadata,
                    },
                    timestamp: new Date().toISOString(),
                  }).catch(e => log.warn('Bus publish failed', e))
                  
                  delete toolcalls[value.toolCallId]
                }
                break
              }

              case "tool-error": {
                const match = toolcalls[value.toolCallId]
                if (match && match.state.status === "running") {
                  await updatePart({
                    ...match,
                    state: {
                      status: "error",
                      input: value.input,
                      error: (value.error as any).toString(),
                      time: {
                        start: match.state.time.start,
                        end: Date.now(),
                      },
                    },
                  })
                  
                  // Emit tool error event for session event streaming (non-blocking)
                  Bus.publish(Server.Event.ToolUsage, {
                    sessionId: assistantMsg.sessionID,
                    agent: assistantMsg.agent || 'unknown',
                    tool: match.tool,
                    action: 'error',
                    description: `Error in ${match.tool}: ${(value.error as any).toString()}`,
                    data: {
                      error: (value.error as any).toString(),
                      input: value.input,
                    },
                    timestamp: new Date().toISOString(),
                  }).catch(e => log.warn('Bus publish failed', e))
                  
                  delete toolcalls[value.toolCallId]
                }
                break
              }

              case "error":
                throw value.error

              case "start-step":
                await updatePart({
                  id: Identifier.ascending("part"),
                  messageID: assistantMsg.id,
                  sessionID: assistantMsg.sessionID,
                  type: "step-start",
                })
                snapshot = await Snapshot.track()
                break

              case "finish-step":
                // CRITICAL FIX: For tool_calls, cache generation ID since onFinish doesn't fire
                const isOpenRouter = assistantMsg.providerID?.toLowerCase() === 'openrouter' || 
                                   assistantMsg.modelID?.includes('openrouter');
                log.error("ADITYA DEBUG 0");
                if (isOpenRouter) {
		  log.error("ADITYA DEBUG 1");
                  
                  // Debug: Print all available objects and their structures
                  log.error("ADITYA DEBUG - value object keys:", Object.keys(value));
                  log.error("ADITYA DEBUG - value.experimental_providerMetadata:", JSON.stringify(value.experimental_providerMetadata));
                  log.error("ADITYA DEBUG - value.providerMetadata:", JSON.stringify(value.providerMetadata));
                  log.error("ADITYA DEBUG - value.response:", JSON.stringify(value.response));
                  
                  const streamResult = (stream as any)._result || (stream as any).result;
                  log.error("ADITYA DEBUG - streamResult:", JSON.stringify(streamResult));
                  log.error("ADITYA DEBUG - stream keys:", Object.keys(stream));
                  
                  // Try multiple ways to extract generation ID
                  const metadata = value.experimental_providerMetadata || value.providerMetadata;
                  const generationId = metadata?.openai?.id || metadata?.openrouter?.generation_id || 
                                     value.response?.id || streamResult?.response?.id;
                  
                  log.error("ADITYA DEBUG - extracted generationId:", generationId);
                  
                  if (generationId && !generationIdCache.has(assistantMsg.sessionID)) {
		    log.error("ADITYA DEBUG 2");
                    generationIdCache.set(assistantMsg.sessionID, generationId);
                    
                    storeGenerationIdInLedger(
                      generationId,
                      assistantMsg.sessionID,
                      "exec",
                      assistantMsg.modelID || "unknown", 
                      "openrouter",
                      "tool_calls"
                    ).catch(e => log.warn("Failed to store tool_calls generation ID in ledger", e));
                    
                    log.info("🔧 TOOL_CALLS GENERATION ID CACHED in finish-step", {
                      sessionID: assistantMsg.sessionID,
                      generationId: generationId,
                      finishReason: "tool_calls"
                    });
                  }
                }
                
                // Use experimental_providerMetadata if available (contains OpenRouter generation_id)
                const metadata = value.experimental_providerMetadata || value.providerMetadata;
                const usage = await getUsage(model, value.usage, metadata, assistantMsg.sessionID)
                assistantMsg.cost += usage.cost
                assistantMsg.tokens = usage.tokens
                await updatePart({
                  id: Identifier.ascending("part"),
                  messageID: assistantMsg.id,
                  sessionID: assistantMsg.sessionID,
                  type: "step-finish",
                  tokens: usage.tokens,
                  cost: usage.cost,
                })
                await updateMessage(assistantMsg)
                if (snapshot) {
                  const patch = await Snapshot.patch(snapshot)
                  if (patch.files.length) {
                    await updatePart({
                      id: Identifier.ascending("part"),
                      messageID: assistantMsg.id,
                      sessionID: assistantMsg.sessionID,
                      type: "patch",
                      hash: patch.hash,
                      files: patch.files,
                    })
                  }
                  snapshot = undefined
                }
                break

              case "text-start":
                currentText = {
                  id: Identifier.ascending("part"),
                  messageID: assistantMsg.id,
                  sessionID: assistantMsg.sessionID,
                  type: "text",
                  text: "",
                  time: {
                    start: Date.now(),
                  },
                }
                // Publish text start event for SSE streaming (non-blocking)
                Bus.publish(Event.MessageTextStart, {
                  sessionId: assistantMsg.sessionID,
                  messageId: assistantMsg.id,
                  partId: currentText.id,
                  timestamp: new Date().toISOString()
                }).catch(e => log.warn('Bus publish failed', e))
                break

              case "text-delta":
                if (currentText) {
                  currentText.text += value.text
                  if (currentText.text) await updatePart(currentText)
                  // Publish text delta event for SSE streaming (non-blocking)
                  Bus.publish(Event.MessageTextDelta, {
                    sessionId: assistantMsg.sessionID,
                    messageId: assistantMsg.id,
                    partId: currentText.id,
                    content: value.text,
                    fullText: currentText.text,
                    timestamp: new Date().toISOString()
                  }).catch(e => log.warn('Bus publish failed', e))
                }
                break

              case "text-end":
                if (currentText) {
                  currentText.text = currentText.text.trimEnd()
                  currentText.time = {
                    start: Date.now(),
                    end: Date.now(),
                  }
                  await updatePart(currentText)
                  // Publish text end event for SSE streaming (non-blocking)
                  Bus.publish(Event.MessageTextEnd, {
                    sessionId: assistantMsg.sessionID,
                    messageId: assistantMsg.id,
                    partId: currentText.id,
                    finalText: currentText.text,
                    timestamp: new Date().toISOString()
                  }).catch(e => log.warn('Bus publish failed', e))
                }
                currentText = undefined
                break

              case "finish":
                assistantMsg.time.completed = Date.now()
                await updateMessage(assistantMsg)
                // Publish message finish event for SSE streaming (non-blocking)
                Bus.publish(Event.MessageFinished, {
                  sessionId: assistantMsg.sessionID,
                  messageId: assistantMsg.id,
                  timestamp: new Date().toISOString()
                }).catch(e => log.warn('Bus publish failed', e))
                break

              default:
                log.info("unhandled", {
                  ...value,
                })
                continue
            }
          }
        } catch (e) {
          log.error("", {
            error: e,
          })
          switch (true) {
            case e instanceof DOMException && e.name === "AbortError":
              assistantMsg.error = new MessageV2.AbortedError(
                { message: e.message },
                {
                  cause: e,
                },
              ).toObject()
              break
            case MessageV2.OutputLengthError.isInstance(e):
              assistantMsg.error = e
              break
            case LoadAPIKeyError.isInstance(e):
              assistantMsg.error = new MessageV2.AuthError(
                {
                  providerID: model.id,
                  message: e.message,
                },
                { cause: e },
              ).toObject()
              break
            case e instanceof Error:
              assistantMsg.error = new NamedError.Unknown({ message: e.toString() }, { cause: e }).toObject()
              break
            default:
              assistantMsg.error = new NamedError.Unknown({ message: JSON.stringify(e) }, { cause: e })
          }
          Bus.publish(Event.Error, {
            sessionID: assistantMsg.sessionID,
            error: assistantMsg.error,
          })
        }
        const p = await getParts(assistantMsg.sessionID, assistantMsg.id)
        for (const part of p) {
          if (part.type === "tool" && part.state.status !== "completed" && part.state.status !== "error") {
            updatePart({
              ...part,
              state: {
                status: "error",
                error: "Tool execution aborted",
                time: {
                  start: Date.now(),
                  end: Date.now(),
                },
                input: {},
              },
            })
          }
        }
        assistantMsg.time.completed = Date.now()
        await updateMessage(assistantMsg)
        return { info: assistantMsg, parts: p }
      },
    }
  }

  export const RevertInput = z.object({
    sessionID: Identifier.schema("session"),
    messageID: Identifier.schema("message"),
    partID: Identifier.schema("part").optional(),
  })
  export type RevertInput = z.infer<typeof RevertInput>

  export async function revert(input: RevertInput) {
    const all = await messages(input.sessionID)
    let lastUser: MessageV2.User | undefined
    const session = await get(input.sessionID)

    let revert: Info["revert"]
    const patches: Snapshot.Patch[] = []
    for (const msg of all) {
      if (msg.info.role === "user") lastUser = msg.info
      const remaining = []
      for (const part of msg.parts) {
        if (revert) {
          if (part.type === "patch") {
            patches.push(part)
          }
          continue
        }

        if (!revert) {
          if ((msg.info.id === input.messageID && !input.partID) || part.id === input.partID) {
            // if no useful parts left in message, same as reverting whole message
            const partID = remaining.some((item) => ["text", "tool"].includes(item.type)) ? input.partID : undefined
            revert = {
              messageID: !partID && lastUser ? lastUser.id : msg.info.id,
              partID,
            }
          }
          remaining.push(part)
        }
      }
    }

    if (revert) {
      const session = await get(input.sessionID)
      revert.snapshot = session.revert?.snapshot ?? (await Snapshot.track())
      await Snapshot.revert(patches)
      if (revert.snapshot) revert.diff = await Snapshot.diff(revert.snapshot)
      return update(input.sessionID, (draft) => {
        draft.revert = revert
      })
    }
    return session
  }

  export async function unrevert(input: { sessionID: string }) {
    log.info("unreverting", input)
    const session = await get(input.sessionID)
    if (!session.revert) return session
    if (session.revert.snapshot) await Snapshot.restore(session.revert.snapshot)
    const next = await update(input.sessionID, (draft) => {
      draft.revert = undefined
    })
    return next
  }

  export async function summarize(input: { sessionID: string; providerID: string; modelID: string }) {
    using abort = lock(input.sessionID)
    const msgs = await messages(input.sessionID)
    const lastSummary = msgs.findLast((msg) => msg.info.role === "assistant" && msg.info.summary === true)
    const filtered = msgs.filter((msg) => !lastSummary || msg.info.id >= lastSummary.info.id)
    const model = await Provider.getModel(input.providerID, input.modelID)
    const app = App.info()
    const system = [
      ...SystemPrompt.summarize(input.providerID),
      ...(await SystemPrompt.environment()),
      ...(await SystemPrompt.custom()),
    ]

    const next: MessageV2.Info = {
      id: Identifier.ascending("message"),
      role: "assistant",
      sessionID: input.sessionID,
      system,
      mode: "build",
      path: {
        cwd: app.path.cwd,
        root: app.path.root,
      },
      summary: true,
      cost: 0,
      modelID: input.modelID,
      providerID: input.providerID,
      tokens: {
        input: 0,
        output: 0,
        reasoning: 0,
        cache: { read: 0, write: 0 },
      },
      time: {
        created: Date.now(),
      },
    }
    await updateMessage(next)

    const processor = createProcessor(next, model.info)
    const stream = streamText({
      maxRetries: 10,
      abortSignal: abort.signal,
      model: model.language,
      onFinish(result) {
        // Extract OpenRouter completion ID from response and inject into metadata
        const isOpenRouter = input.providerID?.toLowerCase() === 'openrouter' || 
                           input.modelID?.includes('openrouter');
        
        // COMPREHENSIVE DEBUGGING for generation ID cache failures
        log.info("=== PROCESSOR GENERATION ID CACHE DEBUG ===", {
          sessionID: next.sessionID,
          providerID: input.providerID,
          modelID: input.modelID,
          isOpenRouter: isOpenRouter,
          hasResponse: !!result.response,
          hasResponseId: !!result.response?.id,
          responseId: result.response?.id,
          finishReason: result.finishReason,
          responseKeys: result.response ? Object.keys(result.response) : 'no_response'
        });
        
        if (isOpenRouter && result.response?.id) {
          log.info("FOUND OPENROUTER COMPLETION ID IN PROCESSOR RESPONSE", { 
            completionId: result.response.id, 
            sessionID: next.sessionID 
          });
          
          // Store generation_id in cache for getUsage function  
          generationIdCache.set(next.sessionID, result.response.id);
          
          // Store in database ledger for tracking and verification
          storeGenerationIdInLedger(
            result.response.id,
            next.sessionID,
            "exec",
            input.modelID || "unknown", 
            "openrouter",
            result.finishReason
          ).catch(e => log.warn("Failed to store exec generation ID in ledger", e));
          
          log.info("✅ PROCESSOR GENERATION ID CACHED", {
            sessionID: next.sessionID,
            generationId: result.response.id,
            cacheSize: generationIdCache.size
          });
          
          // Also inject into metadata (though this might not be passed through)
          if (!result.experimental_providerMetadata) {
            result.experimental_providerMetadata = {};
          }
          if (!result.experimental_providerMetadata.openrouter) {
            result.experimental_providerMetadata.openrouter = {};
          }
          result.experimental_providerMetadata.openrouter.generation_id = result.response.id;
        } else {
          log.warn("❌ PROCESSOR GENERATION ID NOT CACHED", {
            sessionID: next.sessionID,
            reason: !isOpenRouter ? "not_openrouter" : !result.response?.id ? "no_response_id" : "unknown",
            isOpenRouter: isOpenRouter,
            hasResponseId: !!result.response?.id
          });
        }
        
        log.info("=== PROCESSOR STREAMTEXT FINISH DEBUG ===", {
          sessionID: next.sessionID,
          usage: JSON.stringify(result.usage),
          response: JSON.stringify(result.response, null, 2),
          experimental_providerMetadata: JSON.stringify(result.experimental_providerMetadata, null, 2),
          finishReason: result.finishReason,
          text: result.text?.substring(0, 100) + "..."
        });
      },
      messages: [
        ...system.map(
          (x): ModelMessage => ({
            role: "system",
            content: x,
          }),
        ),
        ...MessageV2.toModelMessage(filtered),
        {
          role: "user",
          content: [
            {
              type: "text",
              text: "Provide a detailed but concise summary of our conversation above. Focus on information that would be helpful for continuing the conversation, including what we did, what we're doing, which files we're working on, and what we're going to do next.",
            },
          ],
        },
      ],
    })

    const result = await processor.process(stream)
    return result
  }

  function isLocked(sessionID: string) {
    return state().pending.has(sessionID)
  }

  function lock(sessionID: string) {
    log.info("locking", { sessionID })
    if (state().pending.has(sessionID)) throw new BusyError(sessionID)
    const controller = new AbortController()
    state().pending.set(sessionID, controller)
    return {
      signal: controller.signal,
      async [Symbol.dispose]() {
        log.info("unlocking", { sessionID })
        state().pending.delete(sessionID)

        const isAutoCompacting = state().autoCompacting.get(sessionID) ?? false
        if (isAutoCompacting) {
          state().autoCompacting.delete(sessionID)
          return
        }

        const session = await get(sessionID)
        if (session.parentID) return

        Bus.publish(Event.Idle, {
          sessionID,
        })
      },
    }
  }

  async function getUsage(model: ModelsDev.Model, usage: LanguageModelUsage, metadata?: ProviderMetadata, sessionID?: string) {
    // LOG EVERYTHING for debugging OpenRouter costs
    log.info("=== FULL API CALL DEBUG ===", {
      sessionID,
      model: {
        id: model.id,
        provider: model.provider,
        cost: model.cost
      },
      usage: JSON.stringify(usage),
      metadata: JSON.stringify(metadata, null, 2),
      isOpenRouter: model.provider?.toLowerCase() === 'openrouter' || metadata?.["openrouter"] || model.id?.includes('openrouter')
    });
    
    // Additional debug logging for cache tokens
    const isOpenRouter = model.provider?.toLowerCase() === 'openrouter' || 
                        metadata?.["openrouter"] || 
                        model.id?.includes('openrouter') ||
                        // For title generation, check if we have cached generation_id (indicates OpenRouter)
                        (sessionID && generationIdCache.has(sessionID));
    
    if (isOpenRouter) {
      log.info("=== OPENROUTER CACHE DEBUG ===", {
        sessionID,
        usageFields: Object.keys(usage || {}),
        metadataOpenRouter: metadata?.["openrouter"],
        cachedInputTokens: usage.cachedInputTokens,
        cacheCreationInputTokens: usage.cacheCreationInputTokens,
        allUsageFields: JSON.stringify(usage, null, 2)
      });
    }
    const tokens = {
      input: usage.inputTokens ?? 0,
      output: usage.outputTokens ?? 0,
      reasoning: 0,
      cache: {
        write: (metadata?.["anthropic"]?.["cacheCreationInputTokens"] ??
          // @ts-expect-error
          metadata?.["bedrock"]?.["usage"]?.["cacheWriteInputTokens"] ??
          // OpenRouter cache write tokens
          metadata?.["openrouter"]?.["cache_creation_input_tokens"] ??
          metadata?.["openrouter"]?.["cacheCreationInputTokens"] ??
          // Also check usage object for cache write tokens
          usage.cacheCreationInputTokens ??
          0) as number,
        read: usage.cachedInputTokens ?? 
              metadata?.["openrouter"]?.["cached_input_tokens"] ??
              metadata?.["openrouter"]?.["cachedInputTokens"] ??
              0,
      },
    }

    let cost = 0;
    
    // Use the isOpenRouter variable already declared above for cache debug
    if (isOpenRouter) {
      try {
        // ALWAYS check cache first since metadata is unreliable, then fallback to metadata
        let generationId = null;
        if (sessionID) {
          generationId = generationIdCache.get(sessionID) || generationIdCache.get(sessionID + "_title");
          if (generationId) {
            log.info("Found generation_id in cache", { generationId, sessionID, source: "cache_first" });
          }
        }
        
        // If still not found, try metadata as fallback
        if (!generationId) {
          generationId = metadata?.["openrouter"]?.["generation_id"] || 
                        metadata?.["x-ratelimit"]?.["generation_id"] ||
                        metadata?.["generation_id"];
          if (generationId) {
            log.info("Found generation_id in metadata", { generationId, sessionID, source: "metadata_fallback" });
          }
        }
        
        if (generationId) {
          log.info("Fetching OpenRouter cost for generation", { generationId, sessionID });
          
          // Get OpenRouter API key from Auth system (set via `opencode auth`)
          const openrouterAuth = await Auth.get("openrouter");
          const openrouterKey = openrouterAuth?.type === "api" ? openrouterAuth.key : null;
          if (openrouterKey) {
            // Retry logic: OpenRouter needs time to process the generation data
            let attempts = 0;
            const maxAttempts = 3;
            const delays = [1000, 2000, 3000]; // 1s, 2s, 3s delays
            
            while (attempts < maxAttempts) {
              // Add delay before API call (except first attempt)
              if (attempts > 0) {
                log.info(`Waiting ${delays[attempts-1]}ms before retry ${attempts}`, { generationId, sessionID });
                await new Promise(resolve => setTimeout(resolve, delays[attempts-1]));
              }
              
              const response = await fetch(`https://openrouter.ai/api/v1/generation?id=${generationId}`, {
                headers: {
                  'Authorization': `Bearer ${openrouterKey}`,
                  'Content-Type': 'application/json'
                },
                signal: AbortSignal.timeout(10000) // 10 second timeout
              });
              
              if (response.ok) {
                const data = await response.json();
                log.info("Full OpenRouter generation API response", { generationId, data: JSON.stringify(data), sessionID, attempt: attempts + 1 });
                
                if (data.data && typeof data.data.total_cost === 'number') {
                  cost = data.data.total_cost;
                  logCostStep("OPENROUTER-API-SUCCESS", sessionID, { 
                    generationId, 
                    cost: cost,
                    cacheDiscount: data.data.cache_discount,
                    attempt: attempts + 1
                  });
                  
                  // Clean up cache entry after successful use
                  if (sessionID) {
                    generationIdCache.delete(sessionID);
                  }
                  break; // Success, exit retry loop
                }
              } else if (response.status === 404 && attempts < maxAttempts - 1) {
                log.info(`OpenRouter generation not ready yet (404), will retry`, { generationId, attempt: attempts + 1, sessionID });
              } else {
                log.warn("OpenRouter API error", { generationId, status: response.status, attempt: attempts + 1, sessionID });
                if (attempts === maxAttempts - 1) {
                  log.error("OpenRouter API failed after all retries", { generationId, sessionID });
                }
              }
              
              attempts++;
            }
          } else {
            log.debug("OpenRouter API key not found in auth system");
          }
        } else {
          log.info("METADATA DEBUG - Full metadata object:", { 
            metadata: JSON.stringify(metadata, null, 2), 
            sessionID,
            metadataKeys: metadata ? Object.keys(metadata) : 'null'
          });
        }
      } catch (error) {
        log.error("Error fetching OpenRouter cost", { error: error.message, sessionID });
      }
    }
    
    // Fallback to model.dev costs if OpenRouter fetch failed or not OpenRouter
    if (cost === 0) {
      cost = new Decimal(0)
        .add(new Decimal(tokens.input).mul(model.cost?.input ?? 0).div(1_000_000))
        .add(new Decimal(tokens.output).mul(model.cost?.output ?? 0).div(1_000_000))
        .add(new Decimal(tokens.cache.read).mul(model.cost?.cache_read ?? 0).div(1_000_000))
        .add(new Decimal(tokens.cache.write).mul(model.cost?.cache_write ?? 0).div(1_000_000))
        .toNumber();
        
      if (!isOpenRouter) {
        log.debug("Using model.dev costs", { cost, sessionID, provider: model.provider });
      } else {
        log.warn("Using fallback model.dev costs for OpenRouter", { cost, sessionID });
      }
    }
    
    // Send usage to backend for real-time cost tracking
    if (sessionID && cost > 0) {
      const stage = sessionID.includes("_title") ? "title_generation" : 
                   model.id?.toLowerCase().includes('kimi') ? "exec" : "probe";
      
      logCostStep("WALLET-DEDUCTION", sessionID, { 
        stage: stage,
        model: model.id,
        cost: cost,
        tokens: tokens
      });
      
      await notifyBackendUsage(sessionID, tokens, cost, model).catch(error => {
        log.warn("Failed to notify backend of usage", { sessionID, error: error.message });
      });
    }
    
    return { cost, tokens };
  }

  async function checkUserBalance(sessionID: string, estimatedCost: number = 0.10): Promise<boolean> {
    try {
      const backendUrl = process.env.SCREENER37_BACKEND_URL || 'http://localhost:8000';
      
      const response = await fetch(`${backendUrl}/api/v1/internal/balance-check`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Request': 'opencode-balance-check'
        },
        body: JSON.stringify({
          session_id: sessionID,
          estimated_cost: estimatedCost
        }),
        signal: AbortSignal.timeout(5000)
      });
      
      if (response.ok) {
        const data = await response.json();
        if (!data.hasBalance) {
          log.warn("Insufficient user balance", { sessionID, estimatedCost, userBalance: data.currentBalance });
          return false;
        }
        log.debug("User balance check passed", { sessionID, estimatedCost, userBalance: data.currentBalance });
        return true;
      } else {
        log.warn("Balance check failed", { sessionID, status: response.status });
        // Default to allow if balance check fails (fail-open)
        return true;
      }
    } catch (error) {
      log.error("Balance check error", { sessionID, error: error.message });
      // Default to allow if balance check fails (fail-open)
      return true;
    }
  }

  async function notifyBackendUsage(sessionID: string, tokens: any, cost: number, model: ModelsDev.Model, stage?: string) {
    try {
      const backendUrl = process.env.SCREENER37_BACKEND_URL || 'http://localhost:8000';
      
      // Determine stage - use passed parameter or determine based on model
      let finalStage = stage;
      if (!finalStage) {
        finalStage = 'probe'; // default
        const modelId = model.id?.toLowerCase() || '';
        if (modelId.includes('kimi') || modelId.includes('moonshot') || modelId.includes('deepseek')) {
          finalStage = 'exec';
        }
      }
      
      const usagePayload = {
        session_id: sessionID,
        stage: finalStage,
        model_name: model.id || 'unknown',
        model_provider: model.provider || 'openrouter',
        input_tokens: tokens.input,
        output_tokens: tokens.output,
        cached_tokens: tokens.cache.read + tokens.cache.write,
        cache_read_tokens: tokens.cache.read,
        cache_write_tokens: tokens.cache.write,
        cost_usd: cost,
        requests: 1,
        duration_ms: null,
        timestamp: new Date().toISOString()
      };
      
      const response = await fetch(`${backendUrl}/api/v1/internal/usage`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Internal-Request': 'opencode-usage-tracking'
        },
        body: JSON.stringify(usagePayload),
        signal: AbortSignal.timeout(5000) // 5 second timeout
      });
      
      if (response.ok) {
        log.info("Sent usage data to backend", { sessionID, cost, stage, model: model.id });
      } else {
        log.warn("Backend usage tracking failed", { sessionID, status: response.status });
      }
    } catch (error) {
      log.error("Failed to notify backend of usage", { sessionID, error: error.message });
    }
  }

  export class BusyError extends Error {
    constructor(public readonly sessionID: string) {
      super(`Session ${sessionID} is busy`)
    }
  }

  export async function initialize(input: {
    sessionID: string
    modelID: string
    providerID: string
    messageID: string
  }) {
    const app = App.info()
    await Session.chat({
      sessionID: input.sessionID,
      messageID: input.messageID,
      providerID: input.providerID,
      modelID: input.modelID,
      parts: [
        {
          id: Identifier.ascending("part"),
          type: "text",
          text: PROMPT_INITIALIZE.replace("${path}", app.path.root),
        },
      ],
    })
    await App.initialize()
  }

  // Added by Akshay to get the current tokens in the context window
  export async function getCurrentTokens(sessionID: string): Promise<number> {
    const msgs = await messages(sessionID)
    const previous = msgs.filter((x) => x.info.role === "assistant").at(-1)?.info as MessageV2.Assistant
    
    if (!previous || !previous.tokens) {
      return 0
    }
    
    // Use the same calculation as auto-compaction logic
    return previous.tokens.input + previous.tokens.cache.read + previous.tokens.cache.write + previous.tokens.output
  }
}
