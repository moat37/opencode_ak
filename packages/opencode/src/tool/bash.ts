import { z } from "zod"
import { Tool } from "./tool"
import DESCRIPTION from "./bash.txt"
import { App } from "../app/app"

const MAX_OUTPUT_LENGTH = 100000
const DEFAULT_TIMEOUT = 5 * 60 * 1000
const MAX_TIMEOUT = 10 * 60 * 1000

export const BashTool = Tool.define("bash", {
  description: DESCRIPTION,
  parameters: z.object({
    command: z.string().describe("The command to execute"),
    timeout: z.number().min(0).max(MAX_TIMEOUT).describe("Set this to 120000 milliseconds i.e. 120 seconds.").optional(),
    description: z
      .string()
      .optional()
      .describe(
        "[Optional]: Clear, concise description of what this command does in 5-10 words. Examples:\nInput: ls\nOutput: Lists files in current directory\n\nInput: git status\nOutput: Shows working tree status\n\nInput: npm install\nOutput: Installs package dependencies\n\nInput: mkdir foo\nOutput: Creates directory 'foo'",
      ),
  }),
  async execute(params, ctx) {
    const timeout = Math.min(params.timeout ?? DEFAULT_TIMEOUT, MAX_TIMEOUT)
    const progressInterval = 3000
    const startTime = Date.now()

    const process = Bun.spawn({
      cmd: ["bash", "-c", params.command],
      cwd: App.info().path.cwd,
      maxBuffer: MAX_OUTPUT_LENGTH,
      signal: ctx.abort,
      timeout: timeout,
      stdout: "pipe",
      stderr: "pipe",
    })

    let progressCounter = 0
    let stdoutAcc = ""
    let stderrAcc = ""

    // Send periodic progress updates
    const progressTimer = setInterval(async () => {
      if (await process.exited) return

      progressCounter++
      const elapsed = Date.now() - startTime
      const stdout = await new Response(process.stdout).text()
      const stderr = await new Response(process.stderr).text()
      stdoutAcc += stdout
      stderrAcc += stderr

      await ctx.metadata({
        title: `${params.description} (Running, update #${progressCounter}, elapsed ${Math.round(elapsed / 1000)}s)`,
        metadata: {
          status: "running",
          elapsed: elapsed,
          pid: process.pid,
          stdout: stdoutAcc,
          stderr: stderrAcc,
        },
      })
    }, progressInterval)

    try {
      await process.exited
      clearInterval(progressTimer)

      const stdout = await new Response(process.stdout).text()
      const stderr = await new Response(process.stderr).text()
      stdoutAcc += stdout
      stderrAcc += stderr

      return {
        title: params.command,
        metadata: {
          stderr: stderrAcc,
          stdout: stdoutAcc,
          exit: process.exitCode,
          description: params.description,
          duration: Date.now() - startTime,
        },
        output: [`<stdout>`, stdoutAcc ?? "", `</stdout>`, `<stderr>`, stderrAcc ?? "", `</stderr>`].join("\n"),
      }
    } finally {
      clearInterval(progressTimer)
    }
  },
})
