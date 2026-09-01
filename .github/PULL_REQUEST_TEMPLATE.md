## Summary

<!-- What does this change do, and why? -->

## Related issue / spec

<!-- Link to an issue, docs/specs/*, or docs/adr/* if applicable -->

## Checklist

- [ ] `make test` passes locally
- [ ] `pre-commit run --all-files` passes (ruff, mypy, hygiene hooks)
- [ ] Data quality gates in `src/models/gates.py` still enforced/updated if behavior changed
- [ ] Docs updated (`docs/architecture.md`, `docs/data-dictionary.md`, ADRs) if relevant
- [ ] No real PII or credentials introduced (synthetic data only, seed 42)

## Test plan

<!-- How did you verify this change works? -->
