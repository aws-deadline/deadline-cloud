# Commit Message Format Rules

## Structure
```
<type>(<scope>): <description>

[optional body]
[optional footer]
```

## Types (standardize these)
- `feat`: New features
- `fix`: Bug fixes  
- `chore`: Maintenance, deps (exclude releases)
- `docs`: Documentation changes
- `test`: Test additions/modifications
- `refactor`: Code restructuring
- `ci`: CI/CD changes
- `revert`: Reverting changes

## Scope Guidelines
- Use lowercase, kebab-case: `job-attachments`, `cli`, `gui`, `auth`
- Be specific but concise: `job-attachments` not `JobAttachments`
- Common scopes from history: `cli`, `gui`, `job-attachments`, `auth`, `deps`, `ci`

## Description Rules
- Start with lowercase verb
- No period at end
- Max 50 characters
- Be specific about what changed

## Examples of Improved Messages

**Current:** `feat: Add detailed tooltips to grayed-out submit button (#833)`  
**Better:** `feat(gui): add tooltips to disabled submit button`

**Current:** `fix: authentication status text not visible in light mode (#839)`
**Better:** `fix(auth): show status text in light mode`

**Current:** `test: Add tests for the 'deadline job cancel' command`
**Better:** `test(cli): add job cancel command tests`

**Current:** `chore(deps): update ruff requirement from ==0.12.* to ==0.13.* (#832)`
**Better:** `chore(deps): update ruff to 0.13.*`

## Breaking Changes
- Add `!` after type/scope: `feat(api)!: remove deprecated parameter`
- Include `BREAKING CHANGE:` in footer

## Additional Guidelines
- Remove PR numbers from subject line
- Use imperative mood ("add" not "added")
- Group related changes in single commits
- Separate breaking changes with `!` notation
