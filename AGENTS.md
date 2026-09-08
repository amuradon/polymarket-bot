# Repository Agent Guidelines

This repository contains the Polymarket Bot application.

## Java & Quarkus Development Rules

When working on any Java and Quarkus code in this repository, agents **MUST ALWAYS** follow the specific rules and instructions defined in:

👉 [`.agents/rules/quarkus.md`](.agents/rules/quarkus.md)

### Key Mandates from Quarkus Rules:
- **Extension-First Rule**: Before writing any code, always search for existing Quarkus extensions (`quarkus_searchDocs`, `quarkus_searchTools`). Never implement custom logic if a Quarkus extension exists.
- **User Consultation**: If multiple matching extensions exist, present all options to the user with a recommendation and wait for their choice.
- **Skills Usage**: Load extension skills with `quarkus_skills` prior to writing code or tests.
- **Testing & Dev MCP Tools**: Use Quarkus Dev MCP tools (`quarkus_callTool`, `devui-testing_runTests`, etc.) rather than running ad-hoc Maven commands.
- **No `mvn clean` in Dev Mode**: Never run `mvn clean` or `gradle clean` while dev mode is running.
