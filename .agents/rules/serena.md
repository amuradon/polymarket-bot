# Serena MCP Server & Semantic Code Navigation Rules

When working on code exploration, navigation, refactoring, or precise editing in this project, agents should leverage the **Serena MCP server** tools.

---

## 1. Quick Activation & Session Initialization

1. Call `serena:initial_instructions` to retrieve the active session ID and guidelines.
2. If the project is not active or needs activation, call `serena:activate_project` with:
   - `project`: `"c:\\work\\workspace-java\\polymarket-bot"`
   - `session`: active Serena session ID.

---

## 2. Best Practices & Workflow

- **Token-Efficient Exploration**:
  - Prefer symbolic search and structural overviews (`s.lsp.get_symbols_overview`, `s.lsp.find_symbol`, `s.lsp.find_referencing_symbols`) over reading entire files.
  - Read only the bodies of symbols you need (`include_body=True`) or targeted line spans.
- **Reference-Aware Refactoring**:
  - Use `s.lsp.rename_symbol` and `s.lsp.safe_delete_symbol` for atomic, reference-checked updates across declarations, implementations, references, and imports.
- **Precise Modifications**:
  - Use `s.edit.replace_symbol_body`, `s.edit.insert_after_symbol`, or `s.edit.insert_before_symbol` for symbol-level changes.
  - Use `s.edit.replace_content` or `s.edit.replace_in_files` for surgical regex/string replacements.
- **REPL Execution (`serena:serena_repl`)**:
  - Access Serena facades via `s` (`s.lsp`, `s.edit`, `s.fs`, `s.mem`, `s.cfg`).
  - Batch independent calls to minimize round-trips.
