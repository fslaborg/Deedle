# Contributing to Deedle

Deedle is primarily maintained through agentic development under human guidance. Maintainers discuss
proposed work and then direct coding agents to make the complete change, including implementation,
tests, documentation, benchmarks, and other affected files.

## Start With an Issue

We generally prefer contributions as [GitHub issues] rather than pull requests. Search for an
existing report first. Issues may include proposed changes, patches, or links to forks or branches.
Maintainers may refine the scope and assign the issue to an agent to implement and validate the
complete change.

## Repo Assist

[Repo Assist] is an automated AI assistant that runs regularly in this repository. It may triage or
respond to issues, investigate bugs, suggest improvements, and attempt implementations as draft pull
requests. Its work is identified as automated and remains subject to human review; Repo Assist does
not merge pull requests or make final maintenance decisions.

Maintainers can invoke Repo Assist with `/repo-assist <instructions>` for a specific agentic task,
such as investigating an issue, preparing a fix, adding tests, or updating documentation.

## Pull Requests

Every pull request must have a matching issue that has been discussed with the maintainers. Link the
pull request to that issue and keep it focused. Maintainers may close a pull request and use the issue
as the basis for an agent-produced implementation instead; the submitted analysis and code remain
valuable inputs to that work.

If a pull request is the agreed approach, follow [AGENTS.md], update release notes for user-facing
changes, record relevant before-and-after benchmarks for performance work, and run:

```bash
dotnet tool restore
dotnet paket restore
./build.sh
```

[AGENTS.md]: AGENTS.md
[GitHub issues]: https://github.com/fslaborg/Deedle/issues
[Repo Assist]: https://github.com/githubnext/agentics/blob/main/docs/repo-assist.md