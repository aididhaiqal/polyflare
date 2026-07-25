import { writeFile } from "node:fs/promises";
import { bridgeOpenAIChatRequest } from "./src/bridge.mjs";
import { startFakeAnthropic } from "./src/fake-anthropic.mjs";

const claudeExecutable = process.env.CLAUDE_EXECUTABLE;
if (!claudeExecutable) {
  throw new Error(
    "Set CLAUDE_EXECUTABLE to an installed Claude Code 2.1.218 binary",
  );
}

const fake = await startFakeAnthropic();
try {
  const response = await bridgeOpenAIChatRequest(
    {
      model: "claude-sonnet-4-6",
      messages: [
        {
          role: "system",
          content: "Answer in one short sentence.",
        },
        {
          role: "user",
          content: "This is a content-safe local bridge probe.",
        },
      ],
    },
    {
      claudeExecutable,
      upstreamUrl: fake.baseUrl,
    },
  );

  const report = {
    response,
    captures: fake.captures,
  };
  await writeFile(
    new URL("./capture.json", import.meta.url),
    `${JSON.stringify(report, null, 2)}\n`,
    "utf8",
  );
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
} finally {
  await fake.close();
}
