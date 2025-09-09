import { z } from "zod"
import { Tool } from "./tool"
import DESCRIPTION from "./bash.txt"
import { App } from "../app/app"

const MAX_OUTPUT_LENGTH = 100000
const DEFAULT_TIMEOUT = 5 * 60 * 1000
const MAX_TIMEOUT = 5 * 60 * 1000

export const BashTool = Tool.define("bash", {
  description: DESCRIPTION,
  parameters: z.object({
    command: z.string().describe("The command to execute"),
    //timeout: z.number().min(0).max(MAX_TIMEOUT).describe("Optional timeout in milliseconds").optional(),
    description: z
      .string()
      .optional()
      .describe(
        "User-facing description of what you are doing. Do not mention what you're using or how you are doing it. Use as few words as possible. Use words like 'companies' (i.e. external-facing) instead of 'directories' (i.e. internal-facing). Use present continuous tense. Examples: 'Looking for automobile companies', 'Searching for concall reports containing corporate governance related terms', 'Getting company names for ticker symbols'",
      ),
  }),
  async execute(params, ctx) {
    //const timeout = Math.min(params.timeout ?? DEFAULT_TIMEOUT, MAX_TIMEOUT)
    const timeout = Math.min(DEFAULT_TIMEOUT, MAX_TIMEOUT)

    // Inject session_id for Python commands
    let command = params.command
    if ((command.includes('python3') || command.includes('python')) && (command.includes('annual_report_reader.py') || command.includes('multi_file_reader.py'))) {
      // Add session_id as the last argument
      command = `${command} --session-id "${ctx.sessionID}"`
    }

    const process = Bun.spawn({
      cmd: ["bash", "-c", command],
      cwd: App.info().path.cwd,
      maxBuffer: MAX_OUTPUT_LENGTH,
      signal: ctx.abort,
      timeout: timeout,
      stdout: "pipe",
      stderr: "pipe",
    })
    await process.exited
    const stdout = await new Response(process.stdout).text()
    const stderr = await new Response(process.stderr).text()

    return {
      title: params.command,
      metadata: {
        stderr,
        stdout,
        exit: process.exitCode,
        description: params.description,
      },
      output: [`<stdout>`, stdout ?? "", `</stdout>`, `<stderr>`, stderr ?? "", `</stderr>`].join("\n"),
    }
  },
})
