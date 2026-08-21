# ADR - Architecture Decision Records

This directory contains ADRs documenting significant architectural decisions.

## Template

```markdown
# ADR-001: Decision Title

**Status**: Accepted | Deprecated | Superseded

**Context**: What is the issue that we're seeing?

**Decision**: What is the change that we're proposing?

**Consequences**: What becomes easier or more difficult?
```

## Decisions

### ADR-001: Modular Monolith Architecture

**Status**: Accepted

**Context**: We need an architecture that supports production trading while avoiding premature complexity.

**Decision**: Use a modular monolith with independent worker processes.

**Consequences**:
- Simpler deployment and debugging
- Clear module boundaries
- Can evolve to microservices later if needed
- Workers can be scaled independently if needed
