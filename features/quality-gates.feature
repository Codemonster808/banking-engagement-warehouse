Feature: Quality gates catch seeded defect classes
  Spec: docs/specs/spec-quality-gates.md

  Scenario: a missing required field fails required_fields
    Given a bronze batch with one event missing event_id
    When the required_fields gate runs
    Then the gate fails

  Scenario: duplicate event_id fails duplicates
    Given a bronze batch with two rows sharing event_id e1
    When the duplicates gate runs
    Then the gate fails

  Scenario: an unknown customer fails referential integrity
    Given a bronze batch referencing customer_id unknown
    When the referential_integrity gate runs against known customer cust_000001
    Then the gate fails
