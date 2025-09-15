# Changelog Format Guidelines

## Structure
```
## Version (Date)

### BREAKING CHANGES
* Description (#PR) ([commit]) 
  * Additional context/migration notes

### Features  
* Description (#PR) ([commit])

### Bug Fixes
* Description (#PR) ([commit])

### Experimental
* Description (#PR) ([commit])

### Deprecations
* Description with timeline
```

## Commit Message to Changelog Mapping

### Type Mapping
- `feat:` → **Features** section
- `fix:` → **Bug Fixes** section  
- `feat!:` or `fix!:` → **BREAKING CHANGES** section
- Experimental features → **Experimental** section
- Deprecation notices → **Deprecations** section

### Scope Handling
- Include scope in description: `feat(cli): add new command` → "Add new command (CLI)"
- Group related changes by scope when possible
- Use scope to provide context, not as section headers

## Description Rules

### Sentence Structure
```
[Action Verb] + [Object/Feature] + [Context/Location] + [Benefit/Impact]
```

### Action Verbs by Category

**Adding Features:**
- Add, Introduce, Enable, Support, Implement
- "Add job wait command for monitoring completion"
- "Enable automatic download of job attachments"

**Fixing Issues:**
- Fix, Resolve, Correct, Handle, Prevent
- "Fix authentication status visibility in light mode"
- "Prevent submission dialog closing on exceptions"

**Removing/Changing:**
- Remove, Replace, Update, Refactor, Change
- "Remove deprecated create_job_response parameter"
- "Update queue parameter handling for better validation"

**Improving:**
- Improve, Enhance, Optimize, Streamline
- "Improve response time on Windows config GUI"
- "Enhance S3 timeout error handling"

### Object/Feature Patterns

**Be Specific:**
- ✅ "job wait command" not "command"
- ✅ "authentication status text" not "text"
- ✅ "Windows long paths" not "paths"

**Include Scope Context:**
- ✅ "CLI job cancel command"
- ✅ "GUI submit button tooltips" 
- ✅ "job attachments file permissions"

### Context/Location Modifiers

**When/Where:**
- "during job download"
- "in light mode"
- "on Windows systems"
- "when using storage profiles"

**Conditions:**
- "for jobs with attachments"
- "with identical names"
- "without focus loss"

### Format Guidelines
- **Imperative mood**: "Add", "Fix", "Remove" (not "Added", "Fixed", "Removed")
- **Active voice**: "Fix dialog closing" (not "Dialog closing is fixed")
- **Target length**: 40-60 characters, max 80
- Remove PR numbers from descriptions (keep in metadata)

### Examples

**Current → Improved:**

❌ `authentication status text not visible in light mode`
✅ `Fix authentication status text visibility in light mode`

❌ `Add detailed tooltips to grayed-out submit button`  
✅ `Add tooltips to disabled submit button for better guidance`

❌ `Support automatic download of job attachments output`
✅ `Enable automatic download of completed job outputs`

❌ `bundle gui-submit fails loading bundles with saved queue parameter values`
✅ `Fix bundle loading failure with saved queue parameters in GUI`

## Breaking Changes Guidelines

### Requirements
- Must include migration notes as sub-bullets
- Explain what changed and why
- Provide upgrade path when possible
- Use `!` notation in commit type

### Format
```
* Remove deprecated parameter from API (#123) ([commit])
  * Use new_parameter instead of old_parameter
  * Migration guide: replace X with Y
```

## Experimental Section

### Criteria
- Features marked as experimental in code
- APIs subject to change
- Beta functionality
- Include warning: "These changes are experimental and are subject to change."

## Metadata Standards

### PR References
- Include PR number: `(#123)`
- Include commit hash: `([abc1234])`
- Link format: `([commit](https://github.com/org/repo/commit/hash))`

### Dates
- Use ISO format: `YYYY-MM-DD`
- Match release date, not merge date

## Grouping Rules

### Within Sections
- Group by functional area when possible
- Most impactful changes first
- Related changes together

### Cross-References
- Link breaking changes to deprecations
- Reference related features/fixes

## Quality Checks

### Required Elements
- [ ] Version follows semver
- [ ] Date is accurate
- [ ] All breaking changes documented
- [ ] Migration notes for breaking changes
- [ ] Experimental features clearly marked
- [ ] Consistent verb tense (imperative)
- [ ] No duplicate entries

### Forbidden Elements
- ❌ Commit messages as descriptions
- ❌ Internal/chore changes (unless user-facing)
- ❌ Merge commit references
- ❌ Inconsistent formatting
- ❌ Missing context for breaking changes

## Automation Guidelines

### From Commit Messages
1. Parse conventional commit format
2. Extract type, scope, description
3. Map to appropriate changelog section
4. Clean up description (remove PR refs, improve grammar)
5. Group by scope/functionality
6. Add required metadata

### Manual Review Required
- Breaking change impact assessment
- Migration note accuracy
- Experimental feature classification
- User-facing impact validation
